from unittest import TestCase

from pyro.transformation import closure, prioritized_relations, contexts, \
    existing_join


class TestClosure(TestCase):
    def test_simple(self):
        attributes = {'D'}
        dependencies = [
            {'left': {'A', 'B'}, 'right': {'C'}},
            {'left': {'B', 'C'}, 'right': {'A', 'D'}},
            {'left': {'D'}, 'right': {'E'}},
            {'left': {'C', 'F'}, 'right': {'B'}},
        ]
        self.assertEqual(closure(attributes, dependencies), {'D', 'E'})

    def test_advanced(self):
        attributes = {'A', 'B'}
        dependencies = [
            {'left': {'A', 'B'}, 'right': {'C'}},
            {'left': {'B', 'C'}, 'right': {'A', 'D'}},
            {'left': {'D'}, 'right': {'E'}},
            {'left': {'C', 'F'}, 'right': {'B'}},
        ]
        self.assertEqual(closure(attributes, dependencies),
                         {'A', 'B', 'C', 'D', 'E'})


class TestExistingJoin(TestCase):
    def test_no_intersections(self):
        """
        Check the case when no relations intersect
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'}}

        result = existing_join([r1, r2, r3])
        self.assertEqual(result, [r1])

    def test_one_separate(self):
        """
        Check the case when all but one relations intersect
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'B': 'INT', 'C': 'INT'}}
        r3 = {'name': 'R3', 'attributes': {'B': 'INT', 'D': 'INT'}}
        r4 = {'name': 'R4', 'attributes': {'F': 'INT', 'G': 'INT'}}

        result = existing_join([r1, r2, r3, r4])
        self.assertEqual(result, [r1, r2, r3])

    def test_all_intersecting(self):
        """
        Check the case when all but one relations intersect
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'G': 'INT', 'C': 'INT'}}
        r3 = {'name': 'R3', 'attributes': {'B': 'INT', 'D': 'INT'}}
        r4 = {'name': 'R4', 'attributes': {'F': 'INT', 'G': 'INT'}}
        r5 = {'name': 'R3', 'attributes': {'A': 'INT', 'C': 'INT'}}

        result = existing_join([r1, r2, r3, r4, r5])
        self.assertEqual(result, [r1, r3, r5, r2, r4])


class TestPrioritizedRelations(TestCase):
    def test_all_low_priority(self):
        """
        Check the case when no base relations and dependencies are provided
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'},
              'pk': {'E'}}

        # case 1: no base relations
        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r1, r2, r3],
            base_relations=[],
            dependencies=[],
            all_attributes={'A', 'B', 'C', 'D', 'E', 'F'}))
        self.assertEqual(priorities, (1,) * 3)

        # case 2: one base relation
        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r2, r3],
            base_relations=[r1],
            dependencies=[],
            all_attributes={'A', 'B', 'C', 'D', 'E', 'F'}))
        self.assertEqual(priorities, (1,) * 2)

    def test_high_priority_hit_with_base_relations(self):
        """
        Check the case when some base relations are provided and it makes
        priorities to raise
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'E': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'B': 'INT', 'C': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'},
              'pk': {'E'}}

        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r2, r3],
            base_relations=[r1],
            dependencies=[],
            all_attributes={'A', 'B', 'C', 'D', 'E', 'F'}))
        self.assertEqual(priorities, (2,) * 2)

    def test_highest_priority_hit(self):
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'C'}, 'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'D', 'E'}, 'pk': {'D'}}
        dependencies = [
            {'left': {'A', 'B'}, 'right': {'C'}},
            {'left': {'B', 'C'}, 'right': {'A', 'D'}},
            {'left': {'D'}, 'right': {'E'}}
        ]

        # case 1: excessive attributes
        all_attributes = {'A', 'B', 'C', 'D', 'E', 'F'}
        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r1, r2, r3],
            base_relations=[],
            dependencies=dependencies,
            all_attributes=all_attributes))
        self.assertEqual(priorities, (1, 1, 1))

        # case 2: closure equals set of all DB attributes
        all_attributes = {'A', 'B', 'C', 'D', 'E'}
        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r1, r2, r3],
            base_relations=[],
            dependencies=dependencies,
            all_attributes=all_attributes))
        self.assertEqual(priorities, (3, 1, 1))

    def test_all_priorities_hit(self):
        """
        Check the case when all types of priorities are assigned
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT'},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'D': 'INT', 'E': 'INT'},
              'pk': {'D'}}
        base = {'name': 'R4', 'attributes': {'C': 'INT', 'A': 'INT'},
                'pk': {'D'}}
        dependencies = [
            {'left': {'A', 'B'}, 'right': {'C'}},
            {'left': {'B', 'C'}, 'right': {'A', 'D'}},
            {'left': {'D'}, 'right': {'E'}}
        ]
        all_attributes = {'A', 'B', 'C', 'D', 'E'}

        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r1, r2, r3],
            base_relations=[base],
            dependencies=dependencies,
            all_attributes=all_attributes))
        self.assertEqual(priorities, (3, 2, 1))


class TestContexts(TestCase):
    def test_no_base_1_relation(self):
        """
        Check the simplest case with only one relation being checked for
        context
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        contexts_gen = contexts(all_relations=[r1], base=[], dependencies=[])
        first_context = next(contexts_gen)
        self.assertEqual(first_context, [r1])
        self.assertRaises(StopIteration, next, contexts_gen)

    def test_no_base_2_relations(self):
        """
        Check the simplest case with couple of relations being checked for
        context
        """
        # case 1: lossless join property fails on relations
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'},
              'pk': {'C'}}
        contexts_gen = contexts(all_relations=[r1, r2], base=[],
                                dependencies=[])
        self.assertEqual(next(contexts_gen), [r1])
        self.assertEqual(next(contexts_gen), [r2])
        self.assertRaises(StopIteration, next, contexts_gen)

        # case 2: lossless succeeds
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT'},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'},
              'pk': {'C'}}
        deps = [{'left': {'C'}, 'right': {'D'}}]
        contexts_gen = contexts(all_relations=[r1, r2], base=[],
                                dependencies=deps)
        self.assertEqual(next(contexts_gen), [r1])
        self.assertEqual(next(contexts_gen), [r2])
        self.assertEqual(next(contexts_gen), [r1, r2])
        self.assertRaises(StopIteration, next, contexts_gen)

    def test_with_base_relations(self):
        """
        Check the case with some base relations and additional relations
        being checked for context
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'D': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'A': 'INT', 'C': 'INT'},
              'pk': {'A'}}
        r3 = {'name': 'R3', 'attributes': {'B': 'INT', 'C': 'INT', 'D': 'INT'},
              'pk': {'B', 'C'}}
        deps = [
            {'left': {'A'}, 'right': {'B'}},
            {'left': {'B'}, 'right': {'C'}},
            {'left': {'C', 'D'}, 'right': {'A'}},
        ]
        contexts_gen = contexts(all_relations=[r1, r2, r3], base=['R1'],
                                dependencies=deps)
        self.assertEqual(next(contexts_gen), [r1])
        self.assertEqual(next(contexts_gen), [r1, r2, r3])
        self.assertRaises(StopIteration, next, contexts_gen)
