from copy import deepcopy

from sqlalchemy import Column, Table, MetaData, select
from sqlalchemy.types import Integer, String, DateTime, Text, _Binary, \
    LargeBinary
from sqlalchemy.exc import OperationalError

from pyro.utils import containing_relation


def transform_column_type(column_type):
    """
    Make necessary transformation of `column_type` to one of the base
    SQLAlchemy types or the one with `collation` and `encoding` attributes set
    to default values.

    This transformation is needed due to potential differences in DBMSes in
    use.
    :param column_type: object representing DB column data type
    :return: one of the `sqlalchemy.types` classes
    """
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
    """
    Execute CREATE TABLE on desired DB with specified name/attributes.

    :param name: table name
    :param attributes: dict of (name, type) pairs representing table columns
    :param engine: SQLAlchemy engine to be used
    """
    cube_metadata = MetaData(engine)
    columns = {Column(name, type) for name, type in attributes.iteritems()}
    try:
        Table(name, cube_metadata, *columns).create(engine)
    except OperationalError as e:
        if 'already exists' not in e.args[0]:
            raise


def get_columns_to_select(relations, all_columns):
    """
    Retrieve all those columns from `all_columns` that actually exist in
    `relations` list of relations. Set table name for those columns so they are
    ready for selection query.

    :param relations: list of relations to consider
    :param all_columns: list of columns to filter
    :return: list of resulting columns
    """
    columns = deepcopy(all_columns)
    for column in columns:
        try:
            relation = containing_relation(relations, column.name)
        except ValueError:
            columns.remove(column)
        else:
            column.table = relation['name']
    return columns


def join(engine, relations, attributes):
    metadata = MetaData(engine, reflect=True)
    join_expr = None
    for r in relations:
        table = metadata.tables[r['name']]
        if join_expr is None:
            join_expr = table
        else:
            join_expr = join_expr.join(table)
    select_columns = get_columns_to_select(relations, attributes)
    s = select(select_columns).select_from(join_expr)
    conn = engine.connect()
    result = conn.execute(s)
    return result
