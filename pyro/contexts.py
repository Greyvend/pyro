from itertools import combinations


def is_lossless(db, relation_names):
    """
    Check whether lossless join property of database is held

    @param db: rdb.Rdb object, representing database
    @param relation_names: list of rdb.relation objects
    @return: True if connection without losses is held on current context
    """

    def modify(working_table, left_indices, right_index):
        """
        Make certain modifications in working_table, according to lossless join
        algorithm.
        @param in out working_table: algorithm table structure to be
        modified
        @param left_indices: indices of attributes to be checked for
        equality
        @param right_index: index of attribute to be assigned values for
        all matched rows
        @return: True if some modification occured, False else
        """
        changed = False
        for i, row in enumerate(working_table):
            matching = [i]
            seed = i

            #Search for relations
            for j, lookup_row in enumerate(working_table[i + 1:]):
                for l_index in left_indices:
                    if lookup_row[l_index] != row[l_index]:
                        break
                else: #if break wasn't executed
                    matching.append(j)
                    if lookup_row[right_index][0] == "a":
                        seed = j

            #Assign rows
            if len(matching) > 1:
                for index in matching:
                    if index != seed:
                        working_table[index][right_index] = \
                            working_table[seed][right_index]
                if not changed:
                    changed = True
        return changed

    def is_string_with_a(working_table):
        """
        check if the lossless join condition is held
        @param working_table: algorithm table structure to be
        modified
        @return: True if there is string with only "a" in some row
        """
        for row in working_table:
            for elem in row:
                if elem[0] != "a":
                    break
            else:
                return True
        return False

    all_attrs = db.all_attributes(relation_names)
    deps = db.dependencies(relation_names)

    #Initial table filling
    working_table = []
    for i in range(len(relation_names)):
        working_table.append([])
        for j in range(len(all_attrs)):
            if db.has_attr(relation_names[i], all_attrs[j]):
                working_table[i].append(("a", j))
            else:
                working_table[i].append(("b", i, j))

    #Main algorithm job
    changed = True
    while changed:
        changed = False
        for dep in deps:
            left_indices = tuple(all_attrs.index(elem)
                                 for elem in dep.left())
            right_index = all_attrs.index(dep.right())

            changed_now = modify(working_table, left_indices, right_index)
            changed = changed or changed_now
            if is_string_with_a(working_table):
                return True
    return False


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

