from itertools import combinations

from pyro.lossless import is_lossless


def closure(attributes, dependencies):
    """
    Count attributes closure according to given functional dependencies. See
    Ullman's "Database Systems - The Complete book", p. 75 for definition of
    closure and this algorithm.

    :param attributes: set of attributes to count closure of
    :param dependencies: list of dependencies held on Database
    :return: set of attributes, contained in closure
    """
    result = set(attributes)

    added = True
    while added:
        added = False
        for dep in dependencies:
            if (result.issuperset(dep['left']) and
                    not result.issuperset(dep['right'])):
                result = result.union(dep['right'])
                added = True
    return result


def contexts(db, relation_names):
    def subsets_basic_pk(relation_name):
        """
        check, whether relation data subsets one of base relation by it's pk
        :param relation_name: relation to be compared
        :return: True if it does subset, False else
        """
        for base_name in relation_names:
            if db.inclusion_dependency(relation_name, base_name, db.primary(
                    base_name)): #check inn inclusion_dependency for
                # attribute match
                return True
        return False

    result = []
    other_relation_names = set(db.relation_names()).difference(relation_names)
    all_attrs = db.all_attributes(relation_names)

    #Set priorities
    prioritized_relation_names = []
    for name in enumerate(other_relation_names):
        if closure(db.primary(name), db.dependencies()).issuperset(all_attrs):
            prioritized_relation_names.append((name, 3))
        elif subsets_basic_pk(name):
            prioritized_relation_names.append((name, 2))
        elif set(all_attrs).intersection(db.attributes(name)):
            prioritized_relation_names.append((name, 1))
        else:
            prioritized_relation_names.append((name, 0))
    prioritized_relation_names.sort(key=lambda elem: elem[1], reverse=True)

    #Context pickup from relation combinations
    n = len(prioritized_relation_names)
    for k in range(1, n + 1):
        name_packs_to_add = combinations(prioritized_relation_names, k)
        for names in name_packs_to_add:
            context = relation_names + names
            if is_lossless(db, context):
                result.append(context)
