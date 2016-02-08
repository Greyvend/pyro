from itertools import combinations


def closure(attributes, dependencies):
    """
    count attribute closure according to given functional dependencies
    @param attributes: attributes to count closure of
    @param dependencies: deps of database, that form closure
    @return: set of attributes, contained in closure
    """
    result = set(attributes)

    #Main job
    added = True
    while added:
        added = False
        for dep in dependencies:
            if result.issuperset(dep.left()) and not result.issuperset(dep
                .right()):
                result = result.union(dep.right())
                added = True
    return result


def contexts(db, relation_names):
    def subsets_basic_pk(relation_name):
        """
        check, whether relation data subsets one of base relation by it's pk
        @param relation_name: relation to be compared
        @return: True if it does subset, False else
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

# >>> student_tuples = [
#     ('john', 'A', 15),
#     ('jane', 'B', 12),
#     ('dave', 'B', 10),
#     ]
# >>> sorted(student_tuples, key=lambda student: student[2])   # sort by age
# [('dave', 'B', 10), ('jane', 'B', 12), ('john', 'A', 15)]

