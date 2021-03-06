from functools import partial
from functools import reduce
from itertools import groupby

from pyro import utils
from pyro.utils import min_dict, all_equal, project


def _build_tableau(relations):
    tableau = []
    all_attributes = utils.all_attributes(relations)
    for r in relations:
        tableau.append(
            {attr: (attr,) if attr in r['attributes'] else (attr, r['name'])
             for attr in all_attributes})
    return tableau


def is_clear(row):
    """
    Check that provided tableau row consists only of simple values

    :param row: dictionary representing a tableau row
    :return: True if the row contains all simple values, False otherwise
    """
    for k, v in row.items():
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
            dep_left = partial(project, keys=dep['left'])
            dep_right = partial(project, keys=dep['right'])
            tableau = sorted(tableau, key=lambda r: list(dep_left(r).values()))
            for k, group in groupby(tableau, key=dep_left):
                group = list(group)
                values_to_change = list(map(dep_right, group))
                if not all_equal(values_to_change):
                    changed = True
                    group_min = reduce(min_dict, values_to_change)
                    for row in group:
                        row.update(group_min)
        try:
            next(filter(is_clear, tableau))
            return True
        except StopIteration:
            pass
    return False
