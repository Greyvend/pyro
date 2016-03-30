from itertools import combinations

from pyro.lossless import is_lossless
from pyro import utils


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


def prioritized_relations(relations, base_relations, dependencies,
                          all_attributes):
    """
    Compute relations priorities and sort `relations` by those priorities in
    descending order. Priorities denote likelihood of the relation to satisfy
    lossless join property when joined together with `base_relations`.

    :param relations: list of relations to prioritize
    :param base_relations: list of initial relations
    :param dependencies: list of dependencies that are satisfied by all
        relations
    :param all_attributes: set of all DB attributes
    :return: list of tuples of the following type: (r, N), where r is relations
        element and N is a priority number from set {1, 2, 3}.
    """
    result = []
    for relation in relations:
        if closure(relation['pk'], dependencies).issuperset(all_attributes):
            result.append((relation, 3))
        elif set(utils.all_attributes(base_relations)).intersection(
                relation['attributes']):
            result.append((relation, 2))
        else:
            result.append((relation, 1))
    return sorted(result, key=lambda elem: elem[1], reverse=True)


def contexts(all_relations, base, dependencies):
    """
    Build Contexts - sets of relations that satisfy Lossless join property
    based on the `base` relations provided.

    :param all_relations: list of all relations to add to the context
    :param base: list of strings - names of the relations that are initially in
        the context
    :param dependencies: list of dependencies held in the DB
    :return yield list of relations representing context
    """
    base_relations = [r for r in all_relations if r['name'] in base]
    relations_to_check = [r for r in all_relations if r['name'] not in base]

    relations_to_check, priorities = zip(*prioritized_relations(
        relations_to_check,
        base_relations,
        dependencies,
        utils.all_attributes(all_relations)))

    n = len(relations_to_check)
    for k in range(n + 1):
        relation_packs = combinations(relations_to_check, k)
        for relations in relation_packs:
            context = base_relations + list(relations)
            satisfied_deps = filter(lambda d: set(d['left'] | d['right'])
                                    .issubset(utils.all_attributes(context)),
                                    dependencies)
            if is_lossless(context, satisfied_deps):
                yield context


def build_table_of_joins(context, dependencies, source, destination):
    """
    Build Table of Joins data structure and writes it to the destination DB

    :param context: list of relations representing context to use
    :param dependencies: list of dependencies held
    :param source: SQLAlchemy connection to the source database
    :param destination: SQLAlchemy connection to the warehouse
    """
    pass
