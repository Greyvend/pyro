import operator
from copy import deepcopy
from functools import reduce

from sqlalchemy import Column, Table, MetaData, select, column, delete, \
    distinct, insert, table
from sqlalchemy.sql.elements import and_, or_, between, not_
from sqlalchemy.sql.functions import count
from sqlalchemy.types import Integer, String, DateTime, Text, _Binary, \
    LargeBinary

from pyro.utils import containing_relation, common_keys, chunks
# noinspection PyUnresolvedReferences
from pyro import compilers


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
            col.table = metadata.tables[relation['name']]
            yield col
        except ValueError:
            pass


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
    for table, table_data in metadata.tables.items():
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


def create_table(engine, relation):
    """
    Execute CREATE TABLE on desired DB with specified name/attributes.

    :param engine: SQLAlchemy engine to be used
    :param relation: table name
    """
    cube_metadata = MetaData(engine)
    columns = {Column(name, type) for name, type in
               relation['attributes'].items()}
    t = Table(relation['name'], cube_metadata, *columns)
    t.drop(checkfirst=True)
    t.create(engine)


def _execute(engine, query, *multiparams, **params):
    """
    Execute SQLAlchemy query and then transform query result object into list
    of dicts representing result rows

    :param engine: SQLAlchemy engine to be used
    :param query: SQLAlchemy query
    :return: list of dicts, each representing a row in DB
    """
    with engine.connect() as conn:
        result_proxy = conn.execute(query, *multiparams, **params)
        if result_proxy.returns_rows:
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


def _convert_predicate(p):
    operators = {
        '=': operator.eq,
        '<>': operator.ne,
        '>': operator.gt,
        '<': operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
        'BETWEEN': between,
    }
    op_str = p['operator'].lstrip('NOT ')
    values = (p['value'],) if not isinstance(p['value'], list) else p['value']
    clause = operators[op_str](column(p['attribute']), *values)
    if op_str == p['operator']:
        return clause
    else:
        return not_(clause)


def _to_clause(constraint):
    if constraint is not None:
        if isinstance(constraint, dict):
            return and_(column(k) == v for k, v in constraint.items())
        else:
            return or_(and_(_convert_predicate(predicate)
                            for predicate in conjunct)
                       for conjunct in constraint)
    else:
        return None


def get_data(engine, relation_name, attributes, constraint=None,
             order_by=None):
    """
    Core module function.

    Low level SELECT wrapper that gets specified attributes that
    satisfy logical constraint `where`.

    :param engine: SQLAlchemy engine to be used
    :param relation_name: relation to scan
    :param attributes: list of attribute names or dictionary of name -> types
    :param constraint: logical constraint
    :param order_by: list of attribute names to order result by
    """
    s = select(columns=map(column, attributes), from_obj=table(relation_name),
               whereclause=_to_clause(constraint), order_by=order_by)
    return _execute(engine, s)


def get_rows(engine, relation):
    """
    Obtains all table rows from Database. Performs SELECT under the hood.

    :param engine: SQLAlchemy engine to be used
    :param relation: relation to scan
    """
    return get_data(engine, relation['name'], relation['attributes'].keys())


def delete_rows(engine, relation, rows):
    _rows = list(rows)
    if not _rows:
        return
    row_chunks = list(chunks(_rows, 200))
    for chunk in row_chunks:
        whereclause = or_(and_(column(k) == row[k] for k in row)
                          for row in chunk)
        del_query = delete(table(relation['name'])).where(whereclause)
        _execute(engine, del_query)


def insert_rows(engine, relation, rows):
    _rows = list(rows)
    if not _rows:
        return
    metadata = MetaData(engine, reflect=True)
    insert_query = insert(metadata.tables[relation['name']])
    _execute(engine, insert_query, _rows)


def count_attributes(engine, relation_name, attributes):
    """
    Count amount of distinct values of attributes in the table

    :param engine: SQLAlchemy engine to be used
    :param relation_name: relation to scan
    :param attributes: list of attribute names or dictionary of name -> types

    :return: list of amounts, in the same order as input attributes
    """
    try:
        attr_names = attributes.keys()
    except AttributeError:
        attr_names = attributes
    columns = map(column, attr_names)
    distinct_columns = map(distinct, columns)
    count_distinct_columns = map(count, distinct_columns)
    s = select(columns=count_distinct_columns, from_obj=table(relation_name))
    counts_dict = _execute(engine, s)[0]
    # noinspection PyTypeChecker
    keys = ['count_{}'.format(i + 1) for i in range(len(counts_dict.keys()))]
    return [counts_dict[k] for k in keys]


def project(engine, relation_name, attributes):
    """
    Calculates relation projection on the attributes of the hierarchy, ordering
    result by all the attributes in hierarchy, in order they are presented

    :param engine: SQLAlchemy engine to be used
    :param relation_name: relation to scan
    :param attributes: list of attribute names or dictionary of name -> types

    :return: list of rows, each row is a dict of attr_name -> value pairs
    """
    try:
        attr_names = attributes.keys()
    except AttributeError:
        attr_names = attributes
    return get_data(engine, relation_name, attr_names, order_by=attr_names)
