def all_attributes(relations):
    """
    Set of all attributes of given relations
    :param relations:
    :return:
    """
    attributes = (r['attributes'] for r in relations)
    return set().union(*attributes)


def containing_relation(relations, attribute):
    # type: (list, str) -> dict
    """
    Find relation among provided that contains specified attribute.

    :param relations: list of relations
    :param attribute: name of the attribute
    """
    for r in relations:
        if attribute in r['attributes']:
            return r
    raise ValueError('Attribute was not found in the relations specified')


def project(d, keys):
    """
    Return subdict of given dictionary containing only specified keys.
    :param d:
    :param keys:
    :return:
    """
    return {k: d[k] for k in keys}


def common_keys(d1, d2):
    """
    Return set of keys shared between two dictionaries
    :param d1:
    :param d2:
    :return:
    """
    return set(d1) & set(d2)


def min_dict(d1, d2):
    """
    Based on two input dictionaries build third one with all the values set to
    minimum of `d1`, `d2` across the common keys

    :param d1: first input dict
    :param d2: second input dict
    """
    result = {}
    for key in common_keys(d1, d2):
        result[key] = min(d1[key], d2[key])
    return result


def all_equal(iterable):
    """
    Check that all values of iterable are equal

    :param iterable: any iterable
    :return: True if all values are equal, False otherwise
    """
    first = iterable[0]
    for item in iterable[1:]:
        if cmp(first, item) != 0:
            return False
    return True


def relation_name(attribute_name):
    """
    Extract relation name from extended attribute name

    :param attribute_name: string in a form 'Relation_name.Attribute_name'
    :return: string representing relation name ('Relation_name')
    """
    return attribute_name.split('.')[0]
