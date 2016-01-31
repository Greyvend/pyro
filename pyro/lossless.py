from operator import iadd


def _build_tableau(relations):
    tableau = []
    attrs = (r['attributes'] for r in relations)
    all_attributes = set().union(*attrs)
    for r in relations:
        tableau.append(
            {attr: attr if attr in r['attributes'] else attr + '_' + r['name']
             for attr in all_attributes})
    return tableau


def is_lossless(relations, deps):
    """
    Check whether lossless join property of database is held

    :param relations: list of relations to be checked
    :param deps: list of dependencies that are held on the given set of
        relations
    :return: True if connection without losses is held on current context
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
    tableau = _build_tableau(relations)
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