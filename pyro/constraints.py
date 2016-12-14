def project(constraint, attributes):
    """
    Perform F\X, a contraction projection. It means that from F\X(t) = TRUE it
    follows that F(t) = TRUE, whereas the opposite might not hold.

    :param constraint: logical constraint representation to be used (F)
    :type constraint: list of lists of dicts
    :param attributes: list of attribute names to project to (X)
    :return: modified constraint
    """
    projection = [[p for p in conjunction_clause if p['attribute']
                   in attributes]
                  for conjunction_clause in constraint]
    return list(filter(None, projection))


def not_null(attributes):
    return [[{'attribute': a, 'operator': '<>', 'value': None}
             for a in attributes]]
