from functools import partial
from itertools import groupby


def _build_tableau(relations):
    tableau = []
    attrs = (r['attributes'] for r in relations)
    all_attributes = set().union(*attrs)
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


def equal(iterable):
    first = iterable[0]
    for item in iterable[1:]:
        if cmp(first, item) != 0:
            return False
    return True


def is_clear(d):
    for k, v in d.iteritems():
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
                values_to_change = map(dep_right, group)
                if not equal(values_to_change):
                    changed = True
                    group_min = reduce(min_dict, values_to_change)
                    for row in group:
                        row.update(group_min)
                    clear_rows = filter(is_clear, group)
                    if clear_rows:
                        return True
    return False
