import json

from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL

from pyro import transformation
from pyro.transformation import build_warehouse


config = {}


def column_name(column):
    return column.name


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
    # read RDB schema: all tables info: their attributes and dependencies
    relations = []

    # get multi valued dependencies from the config
    mvd = config.get('multi valued dependencies', [])
    # transform lists of attributes to sets of attributes
    mvd = [{part: set(attributes) for part, attributes in dep.iteritems()}
           for dep in mvd]
    dependencies = mvd

    database_engine = create_engine(URL(**config['database']))
    metadata = MetaData(database_engine, reflect=True)

    # fill dependencies from Primary keys and Unique constraints
    for table, table_data in metadata.tables.iteritems():
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
    app_context = transformation.contexts(relations, base_total,
                                          dependencies).next()

    # connect to the output DB, called "Warehouse"
    warehouse_engine = create_engine(URL(**config['warehouse']))
    for context in contexts:
        build_warehouse(context=context, dependencies=dependencies,
                        database=database_engine, warehouse=warehouse_engine)
