from sqlalchemy import String

from pyro import db
from pyro.transformation import lossless_combinations
from pyro.utils import containing_relation, common_keys

VECTOR_ATTRIBUTE = 'g'
VECTOR_SEPARATOR = ','


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
    Serialize to string containing relation names.

    Example:

        relations = [{"name": "users"}, {"name": "addresses"},
                     {"name": "payments"}, {"name": "pictures"}]

    will return

        "users,addresses,payments,pictures"

    :param relations: input relations
    :type relations: list of dicts
    :return string encoding all relation names
    """
    return VECTOR_SEPARATOR.join(r['name'] for r in relations)


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


def is_empty_attr(context, row, attribute_name):
    """
    Attribute attr in row is considered empty if it has value None and
    its vector doesn't contain any relations having given attribute in their
    schema.

    :param context: context to apply
    :type context: list of dicts representing relations
    :param row: target row
    :type row: dict
    :param attribute_name: target attribute name
    :type attribute_name: str
    :return: True - if the attribute value in row is empty, False otherwise
    """
    if row[attribute_name] is not None:
        return False
    row_relation_names = decode_vector(row)
    row_relations = filter(lambda rel: rel['name'] in row_relation_names,
                           context)
    try:
        containing_relation(row_relations, attribute_name)
    except ValueError:
        return True
    return False


def is_less_or_equal(v1, v2):
    """
    Make comparison of two vectors. Return true if first is less than or equal
    than second. This means that second vector contains all elements from the
    first.

    :param v1: first vector
    :param v2: second vector
    """
    return set(v1).issubset(v2)


def is_subordinate(context, r1, r2):
    if not is_less_or_equal(*map(decode_vector, [r1, r2])):
        return False
    attributes = common_keys(r1, r2)
    attributes.remove(VECTOR_ATTRIBUTE)
    for attr in attributes:
        if r1[attr] != r2[attr] and not is_empty_attr(context, r1, attr):
            return False
    return True


def filter_subordinate_rows(context, tj_data, new_data):
    """
    Filter existing tj_data based on new_data. Return subordinate rows to
    be deleted.

    :param context: context of operation
    :param tj_data: list of rows currently existing in TJ
    :param new_data: list of new rows
    :return: list of rows to delete
    """
    for tj_row in tj_data:
        for new_row in new_data:
            if is_subordinate(context, tj_row, new_row):
                yield tj_row


def compose_tj_name(context):
    return 'TJ_' + '_'.join(sorted(r['name'] for r in context))


def build(context, dependencies, source, cube):
    """
    Build Table of Joins and write it to the destination DB

    :param context: list of relations representing context to use
    :param dependencies: list of dependencies held
    :param source: SQLAlchemy engine for source DB
    :param cube: SQLAlchemy engine for cube DB
    """
    # create TJ in destination DB
    tj_name = compose_tj_name(context)
    attributes = get_attributes(context, dependencies)
    # add vector attribute holding information about participating relations
    full_schema = attributes.copy()
    full_schema.update({VECTOR_ATTRIBUTE: String})
    tj = {'name': tj_name, 'attributes': full_schema}
    db.create_table(cube, tj)

    # fill TJ with data
    relations_packs = list(lossless_combinations(context, dependencies))
    if context not in relations_packs:
        relations_packs.append(context)
    for relations in relations_packs:
        # Execute JOIN of required source db tables
        join_data = db.natural_join(source, relations, attributes)
        # not using functional approach here to avoid data copying
        for row in join_data:
            row[VECTOR_ATTRIBUTE] = VECTOR_SEPARATOR.join(r['name']
                                                          for r in relations)
        tj_data = db.get_rows(cube, tj)
        rows_to_delete = filter_subordinate_rows(context, tj_data, join_data)
        db.delete_rows(cube, tj, rows_to_delete)
        db.insert_rows(cube, tj, join_data)
