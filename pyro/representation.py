from pyro import db
from pyro.tj import compose_tj_name
from pyro.utils import attribute_name


def attribute_values(engine, attribute, hierarchy):
    """
    Calculates all available values of the given attribute in the DB
    referenced by `engine`.

    :param engine: SQLAlchemy engine to be used
    :param attribute:
    :param hierarchy:

    :returns sorted list of textual representations of attribute values
    """
    # find an attribute in hierarchy
    #
    return []


def _get_hierarchy(engine, relation_name, dimension):
    """
    Calculate list of attribute names in the order they should appear in the
    table.

    :param engine: SQLAlchemy engine to be used
    :param relation_name: name of relation to get hierarchy from
    :param dimension: configuration value of user defined dimension

    :return: list of attribute names in correct order. First attribute to be
    used as top level in hierarchy, the last one is bottom level
    """
    # Count amount of different values in each of attributes
    attributes = list(map(attribute_name, dimension['attributes']))
    counts = db.count_attributes(engine, relation_name, attributes)
    counted_attributes = zip(counts, attributes)
    sorted_attributes = sorted(counted_attributes)
    return list(zip(*sorted_attributes))[1]


def _get_slice(engine, hierarchy):
    pass


def transpose(slice):
    pass


def _build(engine, contexts, dimensions):
    """
    Create logical representation of the cross table

    :param engine: SQLAlchemy engine to be used
    :param contexts: list of contexts (lists of relations)
    :param dimensions: configuration value of user defined dimensions
    :type dimensions: list of strings

    :return: in-memory representation of the result table
    """
    table = []
    assert len(contexts) == 3
    # section 1: build top part of the header (Y dimension)
    hierarchy_y = _get_hierarchy(engine, compose_tj_name(contexts[0]),
                                 dimensions[0])
    slice = _get_slice(engine, hierarchy_y)
    part_y = transpose(slice)
    table.extend(part_y)
    # section 2: build side part of the header (X dimension) and table body
    # TODO: build X & set measures
    return table


def _to_html(table):
    return '<html></html>'


def create(engine, contexts, dimensions, file_name):
    # do calculations and prepare HTML code
    table = _build(engine, contexts, dimensions)
    html_table = _to_html(table)
    # write to a file
    with open(file_name, 'w') as html_file:
        html_file.write(html_table)
