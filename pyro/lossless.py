from functools import partial
from itertools import groupby

from pyro import utils


def _build_tableau(relations):
    tableau = []
    all_attributes = utils.all_attributes(relations)
    for r in relations:
        tableau.append(
            {attr: attr if attr in r['attributes'] else attr + '_' + r['name']
             for attr in all_attributes})
    return tableau


def min_dict(d1, d2):
    """
    Based on two input dictionaries build third one with all the values set to
    minimum of `d1`, `d2` across the common keys

    :param d1: first input dict
    :param d2: second input dict
    """
    result = {}
    intersected_keys = set(d1) & set(d2)
    for key in intersected_keys:
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


def is_clear(row):
    """
    Check that provided tableau row consists only of simple values

    :param row: dictionary representing a tableau row
    :return: True if the row contains all simple values, False otherwise
    """
    for k, v in row.iteritems():
        if len(v) != 1:
            return False
    return True


def is_lossless(relations, deps):
    """
    Check whether lossless join property of database is held

    :param relations: list of relations to be checked
    :param deps: list of dependencies that are held on the given set of
        relations
    :return: True if connection without losses is held on current context
    """
    tableau = _build_tableau(relations)

    changed = True
    while changed:
        changed = False
        for dep in deps:
            def project(d, keys): return {k: d[k] for k in keys}
            dep_left = partial(project, keys=dep['left'])
            dep_right = partial(project, keys=dep['right'])
            tableau = sorted(tableau, key=dep_left)
            for k, group in groupby(tableau, key=dep_left):
                group = list(group)
                values_to_change = map(dep_right, group)
                if not all_equal(values_to_change):
                    changed = True
                    group_min = reduce(min_dict, values_to_change)
                    for row in group:
                        row.update(group_min)
                    clear_rows = filter(is_clear, group)
                    if clear_rows:
                        return True
    return False
