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
