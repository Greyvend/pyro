def attributes(relations):
    """
    Set of all attributes of given relations
    :param relations:
    :return:
    """
    all_attributes = (r['attributes'] for r in relations)
    return set().union(*all_attributes)
