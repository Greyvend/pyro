from operator import itemgetter

from pyro.constraints.domains import factory


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


def _is_predicate_domain_included(p1, p2):
    """
    Prime function performing predicate domain comparison.

    :param p1: first predicate
    :type p1: dict
    :param p2: second predicate
    :type p2: dict
    :return: True if domain of p2 is a superset of p1's domain
    """
    if p1['attribute'] != p2['attribute']:
        return False
    dom_1 = factory(p1)
    dom_2 = factory(p2)
    return dom_1.issubset(dom_2)


def _is_conjunction_clause_domain_included(clause_1, clause_2):
    """
    Find out whether M(clause_1) is a subset of M(clause_2),
    where M - constraint domain. It holds iff clause_1 -> clause_2 ~ TRUE.

    :param clause_1: first conjunction clause
    :type clause_1:  lists of dicts
    :param clause_2: second conjunction clause
    :type clause_2:  list of lists of dicts
    :return: True if M(clause_1) is subset of M(clause_2), False otherwise
    """
    for predicate_2 in clause_2:
        for predicate_1 in clause_1:
            if _is_predicate_domain_included(predicate_1, predicate_2):
                break
        else:
            return False
    return True


def is_domain_included(c1, c2):
    """
    Find out whether M(c1) is a subset of M(c2), where M - constraint domain.
    It holds iff c1 -> c2 ~ TRUE.

    :param c1: first logical constraint
    :type c1:  list of lists of dicts
    :param c2: second logical constraint
    :type c2:  list of lists of dicts
    :return: True if M(c1) is subset of M(c2), False otherwise
    """
    if not c2:
        return True
    if not c1:
        return False
    for conjunction_clause_1 in c1:
        for conjunction_clause_2 in c2:
            if _is_conjunction_clause_domain_included(conjunction_clause_1,
                                                      conjunction_clause_2):
                break
        else:
            return False
    return True


def _conjunction_clauses_equal(cc1, cc2):
    if len(cc1) != len(cc2):
        return False
    cc1_sorted = sorted(cc1, key=itemgetter('attribute', 'operation', 'value'))
    cc2_sorted = sorted(cc2, key=itemgetter('attribute', 'operation', 'value'))
    pairs = zip(cc1_sorted, cc2_sorted)
    return all(p1 == p2 for p1, p2 in pairs)


def equal(c1, c2):
    """
    Check whether two constrains are identical.

    :param c1: first logical constraint
    :type c1:  list of lists of dicts
    :param c2: second logical constraint
    :type c2:  list of lists of dicts
    :return: True if c1 completely equals c2, False otherwise
    """
    if len(c1) != len(c2):
        return False
    for conjunction_clause_1 in c1:
        for conjunction_clause_2 in c2:
            if _conjunction_clauses_equal(conjunction_clause_1,
                                          conjunction_clause_2):
                break
        else:
            return False
    return True
