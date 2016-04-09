from copy import deepcopy

from sqlalchemy import Column, Table, MetaData, select

from pyro.transformation import lossless_combinations
from pyro.utils import containing_relation


def get_attributes(context, dependencies):
    """
    Calculate columns that are needed to be selected from given relations

    Attributes that go to the TJ:
    - dimension attributes
    - key attributes of Measure attribute. These are considered covered by
        functional dependencies
    - any common attributes of context relations
    - binary vector of relations contributing to given row

    :param context:
    :param dependencies:
    :return:
    """
    # TODO: filter attributes to only pick needed ones
    attributes = dict()
    for relation in context:
        attributes.update(relation['attributes'])
    return attributes


def _build_schema(context, dependencies):
    """
    Every TJ consists from the relations of the context

    :param context:
    :return:
    """
    name = 'TJ_' + '_'.join(r['name'] for r in context)
    attributes = get_attributes(context, dependencies)
    columns = {Column(name, type) for name, type in attributes.iteritems()}
    return name, columns


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


def filter_subordinate_rows(data):
    return data  # STUB


def build(context, dependencies, source, cube):
    """
    Build Table of Joins and write it to the destination DB

    :param context: list of relations representing context to use
    :param dependencies: list of dependencies held
    :param source: SQLAlchemy engine for source DB
    :param cube: SQLAlchemy engine for cube DB
    """
    # create TJ schema
    cube_metadata = MetaData(cube)
    table_name, columns = _build_schema(context, dependencies)
    Table(table_name, cube_metadata, *columns).create(cube)

    # fill TJ with data
    tj_data = []
    conn = source.connect()
    source_metadata = MetaData(source, reflect=True)
    relations_gen = lossless_combinations(context, dependencies)
    for relations in relations_gen:
        # Execute JOIN of required source db tables
        join = None
        for r in relations:
            if join is None:
                join = source_metadata.tables[r['name']]
            else:
                join = join.join(source_metadata.tables[r['name']])
        # join = metadata.tables['film'].join(metadata.tables['film_category'])
        select_columns = get_columns_to_select(relations, columns)
        s = select(select_columns).select_from(join)
        result = conn.execute(s)
        tj_data = filter_subordinate_rows(tj_data.extend(result))
