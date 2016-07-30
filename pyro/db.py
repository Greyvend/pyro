from copy import deepcopy

from sqlalchemy import Column, Table, MetaData, select, column
from sqlalchemy import table
from sqlalchemy.sql.elements import and_
from sqlalchemy.types import Integer, String, DateTime, Text, _Binary, \
    LargeBinary
from sqlalchemy.exc import OperationalError

from pyro.utils import containing_relation, common_keys


def _transform_column_type(column_type):
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


def _attrs_to_columns(metadata, relations, attributes):
    """
    Retrieve all those columns from `attributes` that actually exist in
    `relations` list of relations. Set table name for those columns so they are
    ready for selection query.

    :param metadata: SQLAlchemy metadata bound to the DB engine
    :param relations: list of relations to consider
    :param attributes: list of attributes to filter
    :return: list of resulting columns
    :rtype: sqlalchemy.Column
    """
    columns = map(column, attributes.keys())
    for col in columns:
        try:
            relation = containing_relation(relations, col.name)
        except ValueError:
            columns.remove(col)
        else:
            col.table = metadata.tables[relation['name']]
    return columns


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
        attributes = {column.name: _transform_column_type(column.type)
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


def create_table(engine, name, attributes):
    """
    Execute CREATE TABLE on desired DB with specified name/attributes.

    :param engine: SQLAlchemy engine to be used
    :param name: table name
    :param attributes: dict of (name, type) pairs representing table columns
    """
    cube_metadata = MetaData(engine)
    columns = {Column(name, type) for name, type in attributes.iteritems()}
    try:
        Table(name, cube_metadata, *columns).create(engine)
    except OperationalError as e:
        if 'already exists' not in e.args[0]:
            raise


def _execute(engine, query):
    """
    Execute SQLAlchemy query and then transform query result object into list
    of dicts representing result rows

    :param engine: SQLAlchemy engine to be used
    :param query: SQLAlchemy query
    :return: list of dicts, each representing a row in DB
    """
    conn = engine.connect()
    result_proxy = conn.execute(query)
    return [dict(r) for r in result_proxy.fetchall()]


def natural_join(engine, relations, attributes):
    metadata = MetaData(engine, reflect=True)

    bin_exprs = ()
    for i, r in enumerate(relations):
        next_relations = deepcopy(relations[i+1:])
        for r2 in next_relations:
            common_attrs = common_keys(r['attributes'], r2['attributes'])
            bin_exprs = reduce(
                lambda t, a: t + (metadata.tables[r['name']].columns[a] ==
                                  metadata.tables[r2['name']].columns[a],),
                common_attrs, bin_exprs)

    where_expr = and_(*bin_exprs)
    columns = _attrs_to_columns(metadata, relations, attributes)
    s = select(columns).where(where_expr)
    return _execute(engine, s)


def get_rows(engine, relation):
    """
    Obtains all table rows from Database. Performs SELECT under the hood.

    :param engine: SQLAlchemy engine to be used
    :param relation: relation to scan
    """
    s = select(columns=map(column, relation['attributes'].keys()),
               from_obj=table(relation['name']))
    # s = table(relation['name']).select(relation['attributes'].keys())
    return _execute(engine, s)
