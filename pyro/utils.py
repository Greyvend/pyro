def all_attributes(relations):
    """
    Set of all attributes of given relations
    :param relations:
    :return:
    """
    attributes = (r['attributes'] for r in relations)
    return set().union(*attributes)
