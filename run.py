import json
from copy import deepcopy

from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.types import Integer, String, DateTime, Text, _Binary, \
    LargeBinary
from sqlalchemy.engine.url import URL

# noinspection PyUnresolvedReferences
from pyro import transformation, compilers
from pyro.utils import all_attributes


config = {}


def column_name(column):
    return column.name


def transform_column_type(column_type):
    if isinstance(column_type, Integer):
        return Integer
    elif isinstance(column_type, (String, DateTime)):
        return Text
    elif isinstance(column_type, _Binary):
        return LargeBinary
    new_type = deepcopy(column_type)
    new_type.collation = None
    new_type.encoding = 'utf-8'
    return new_type


def relation_name(attribute):
    """
    Extract relation name from extended attribute

    :param attribute: string in a form 'Relation_name.Attribute_name'
    :return: string representing relation name ('Relation_name')
    """
    return attribute.split('.')[0]


if __name__ == '__main__':
    with open('config.json') as config_file:
        config = json.load(config_file)

    relations = []  # all tables info: their attribute names and dependencies
    attributes = dict()  # store attributes name -> data type mapping

    # get multi valued dependencies from the config
    mvd = config.get('multi valued dependencies', [])
    # transform lists of attributes to sets of attributes
    mvd = [{part: set(attributes) for part, attributes in dep.iteritems()}
           for dep in mvd]
    dependencies = mvd

    source_engine = create_engine(URL(**config['source_db']))
    metadata = MetaData(source_engine, reflect=True)

    # fill dependencies from Primary keys and Unique constraints
    for table, table_data in metadata.tables.iteritems():
        # unite existing attributes data with this table's attributes
        attributes.update({column.name: transform_column_type(column.type)
                           for column in table_data.columns})

        pk = set(map(column_name, table_data.primary_key.columns))
        all_columns = set(map(column_name, table_data.columns))

        # fill relation
        r = {'name': table, 'attributes': all_columns, 'pk': pk}
        relations.append(r)

        # fill PK dependency
        primary_key_dep = {
            'left': pk,
            'right': all_columns - pk
        }
        dependencies.append(primary_key_dep)

        # fill Unique dependencies
        unique_indexes = filter(lambda index: index.unique, table_data.indexes)
        for i in unique_indexes:
            key = set(map(column_name, i.columns))
            unique_dep = {
                'left': key,
                'right': all_columns - key
            }
            dependencies.append(unique_dep)

    measure = config['measure']

    # Start transformation
    # build contexts
    contexts = []
    base_total = {relation_name(measure)}
    # build dimension contexts
    for dimension in config['dimensions']:
        # extract relation names from extended attribute names in config
        base = set(map(relation_name, dimension['attributes']))
        base_total |= base
        # for now pick first found context
        context = transformation.contexts(relations, base, dependencies).next()
        contexts.append(context)

    # finally build application context
    try:
        app_context = transformation.contexts(relations, base_total,
                                              dependencies).next()
    except StopIteration:
        # no combination satisfies Lossless Join property. Pick all relations
        app_context = relations

    # we treat application context as just another context.
    contexts.append(app_context)

    # connect to the output DB
    cube_engine = create_engine(URL(**config['cube_db']))
    metadata = MetaData()

    # create TJ schemas.
    # Every TJ consists from the relations of the context
    # Attributes that go to the TJ:
    # - dimension attributes
    # - key attributes of Measure attribute
    # - any common attributes of context relations
    print "we are building TJ now"
    for context in contexts:
        if context == app_context:
            print 'stop na sek'
        context_attributes = all_attributes(context)
        # TODO: filter attributes to only pick needed ones
        tj_attributes = {key: attributes[key] for key in context_attributes}
        columns = [Column(name, type) for name, type in
                   tj_attributes.iteritems()]
        tj = Table('TJ_' + '_'.join(r['name'] for r in context), metadata,
                   *columns)
    metadata.create_all(cube_engine)

    for context in contexts:
        transformation.build_table_of_joins(context=context,
                                            dependencies=dependencies,
                                            source=source_engine,
                                            destination=cube_engine)
