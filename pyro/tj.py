from pyro import db
from pyro.transformation import lossless_combinations


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


def filter_subordinate_rows(data):
    return data  # TODO: Algorithm 2 from the big paper


def build(context, dependencies, source, cube):
    """
    Build Table of Joins and write it to the destination DB

    :param context: list of relations representing context to use
    :param dependencies: list of dependencies held
    :param source: SQLAlchemy engine for source DB
    :param cube: SQLAlchemy engine for cube DB
    """
    # create TJ in destination DB
    name = 'TJ_' + '_'.join(r['name'] for r in context)
    attributes = get_attributes(context, dependencies)
    db.create_table(name, attributes, cube)

    # fill TJ with data
    tj_data = []
    relations_gen = lossless_combinations(context, dependencies)
    for relations in relations_gen:
        # Execute JOIN of required source db tables
        join_data = db.natural_join(relations, attributes)
        tj_data = filter_subordinate_rows(tj_data.extend(result))
