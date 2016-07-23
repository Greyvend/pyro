def all_attributes(relations):
    """
    All attributes found in given relations

    :type relations: list
    :param relations: relations to scan
    :return: dict, containing all common attributes
    """
    attribute_dicts = (r['attributes'] for r in relations)
    # merge attribute dictionaries
    return {k: v for attrs in attribute_dicts for k, v in attrs.iteritems()}


def containing_relation(relations, attribute_name):
    # type: (list, str) -> dict
    """
    Find relation among provided that contains specified attribute.

    :param relations: list of relations
    :param attribute_name: name of the attribute to search
    """
    for r in relations:
        if attribute_name in r['attributes']:
            return r
    raise ValueError('Attribute was not found in the relations specified')


def project(d, keys):
    """
    Return subdict of given dictionary containing only specified keys.
    :param d: dictionary
    :param keys: iterable with key names
    :return: dictionary with specified keys and values from initial dictionary
    """
    return {k: d[k] for k in keys}


def common_keys(d1, d2):
    """
    Return set of keys shared between two dictionaries
    :param d1: dictionary
    :param d2: dictionary
    :return: set of common key names
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
