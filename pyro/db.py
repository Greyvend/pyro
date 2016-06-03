from copy import deepcopy

from sqlalchemy import Column, Table, MetaData
from sqlalchemy.types import Integer, String, DateTime, Text, _Binary, \
    LargeBinary
from sqlalchemy.exc import OperationalError


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


def get_schema(engine):
    """
    Obtain DB schema and transform it into Python types.

    The following naming conventions are used:
    'table' - source representation of table entity. After transformation
    it becomes a 'relation'.
    'column' - source representation of single table column. After
    transformation it becomes an 'attribute'.

    :param engine: SQLAlchemy object with Database schema information
    :rtype: tuple
    :return list of relations, list of dependencies
    """
    metadata = MetaData(engine, reflect=True)

    relations = []
    dependencies = []
    # fill dependencies from Primary keys and Unique constraints
    for table, table_data in metadata.tables.iteritems():
        # update relations
        attributes = {column.name: transform_column_type(column.type)
                      for column in table_data.columns}
        pk = {column.name for column in table_data.primary_key.columns}

        # Update dependencies
        # PK dependency
        dependencies.append({'left': pk, 'right': set(attributes) - pk})

        # Unique dependencies
        unique_indexes = filter(lambda index: index.unique,
                                table_data.indexes)
        for i in unique_indexes:
            key = {column.name for column in i.columns}
            dependencies.append({'left': pk, 'right': set(attributes) - key})

        # Update relations
        r = {'name': table, 'attributes': attributes, 'pk': pk}
        relations.append(r)
    return relations, dependencies


def create_table(name, attributes, engine):
    cube_metadata = MetaData(engine)
    columns = {Column(name, type) for name, type in attributes.iteritems()}
    try:
        Table(name, cube_metadata, *columns).create(engine)
    except OperationalError as e:
        if 'already exists' not in e.args[0]:
            raise
