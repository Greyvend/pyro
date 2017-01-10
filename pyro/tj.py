from operator import itemgetter

import logging
from sqlalchemy import String

from pyro import db
from pyro.cache import Cache
from pyro.constraints import operations as constraint_operations
from pyro.transformation import lossless_combinations
from pyro.utils import all_attributes, process_value, random_str

VECTOR_ATTRIBUTE = 'g'
VECTOR_SEPARATOR = ';separator;'
VECTOR_MAX_LENGTH = 10000

logger = logging.getLogger(__name__)


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


def encode_vector(relations):
    """
    Serialize to string containing lists of all attribute names.

    :param relations: input relations
    :type relations: list of dicts
    :return string encoding all attributes present in all relations, with
        relations sorted by their names
    """
    attribute_strings = (str(sorted(r['attributes'].keys()))
                         for r in sorted(relations,
                                         key=lambda r: r['name']))
    return VECTOR_SEPARATOR.join(attribute_strings)


def decode_vector(row):
    """
    Deserialize relation names from string attribute in a given row.

    Example vector attribute in a row:

        "users,addresses,payments,pictures"

    :param row: target row
    :type row: dict
    :return list of relation names encoded in vector
    """
    return row[VECTOR_ATTRIBUTE].split(VECTOR_SEPARATOR)


def is_empty_attr(row, attribute_name):
    """
    Attribute attr in row is considered empty if it has value None and
    its vector doesn't contain any relations having given attribute in their
    schema.

    :param row: target row
    :type row: dict
    :param attribute_name: target attribute name
    :type attribute_name: str
    :return: True - if the attribute value in row is empty, False otherwise
    """
    if row[attribute_name] is not None or \
                    attribute_name in row[VECTOR_ATTRIBUTE]:
        return False
    return True


def is_vector_less(r1, r2):
    """
    Make comparison of two vectors. Return true if first is less than or equal
    than second. This means that second vector contains all elements from the
    first.

    :param r1: first row
    :param r2: second row
    """
    second_vector = r2[VECTOR_ATTRIBUTE]
    for vector_part in decode_vector(r1):
        if vector_part not in second_vector:
            return False
    return True


def is_subordinate(r1, r2):
    if not is_vector_less(r1, r2):
        return False
    attributes = list(r1.keys())
    attributes.remove(VECTOR_ATTRIBUTE)
    for attr in attributes:
        value_1 = process_value(r1[attr])
        value_2 = process_value(r2.get(attr))
        if value_1 != value_2 and not is_empty_attr(r1, attr):
            return False
    return True


def filter_subordinate_rows(tj_data, new_data):
    """
    Filter existing tj_data based on new_data. Return subordinate rows to
    be deleted.

    :param tj_data: list of rows currently existing in TJ
    :param new_data: list of new rows
    :return: list of rows to delete
    """
    for tj_row in tj_data:
        for new_row in new_data:
            if is_subordinate(tj_row, new_row):
                yield tj_row


def compose_table_name():
    return 'TJ_' + random_str(10)


def create_tj_schema(context, dependencies):
    tj_name = compose_table_name()
    full_schema = get_attributes(context, dependencies)
    # add vector attribute holding information about participating relations
    full_schema.update({VECTOR_ATTRIBUTE: String(VECTOR_MAX_LENGTH)})
    relation = {'name': tj_name, 'attributes': full_schema}
    return relation


def build(context, dependencies, constraint, source, cube, cache_file):
    """
    Build Table of Joins and write it to the destination DB

    :param context: list of relations representing context to use
    :param dependencies: list of dependencies held
    :param constraint: logical constraint representation to be used
    :type constraint: list of lists of dicts
    :param source: SQLAlchemy engine for source DB
    :param cube: SQLAlchemy engine for cube DB
    :param cache_file: file name of the cache file to be used
    """
    cache = Cache(cube, cache_file)
    cached_tj = cache.full_match(context, constraint)
    if cached_tj:
        return cached_tj

    tj = create_tj_schema(context, dependencies)
    db.create_table(cube, tj)

    cache.enable(context, constraint)
    cache.add(tj, context, constraint)

    # fill TJ with data
    relations_packs = list(lossless_combinations(context, dependencies))
    if context not in relations_packs:
        relations_packs.append(context)
    for relations in relations_packs:
        logger.debug('Using relations {}'.format(list(map(itemgetter('name'),
                                                          relations))))
        vector = encode_vector(relations)
        projected_constraint = constraint_operations.project(
            constraint, all_attributes(relations))
        filter_constraint = [[{'attribute': 'g', 'operation': '=',
                               'value': vector}]]
        if cache.enabled and cache.contains_context(relations):
            cache.restore(tj, projected_constraint, filter_constraint)
        else:
            attributes = tj['attributes'].copy()
            del attributes[VECTOR_ATTRIBUTE]
            join_data = db.natural_join(source, relations, attributes)
            # not using functional approach here to avoid data copying
            for row in join_data:
                row[VECTOR_ATTRIBUTE] = vector
            tj_data = db.get_rows(cube, tj)
            rows_to_delete = filter_subordinate_rows(tj_data, join_data)
            db.delete_rows(cube, tj, rows_to_delete)
            db.insert_rows(cube, tj, join_data)
            if projected_constraint:
                db.delete_unsatisfied(cube, tj, projected_constraint,
                                      filter_constraint)
    return tj
