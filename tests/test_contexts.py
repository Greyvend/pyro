from unittest import TestCase

from pyro.contexts import closure, prioritized_relations


class TestClosure(TestCase):
    def test_simple(self):
        attributes = {'D'}
        dependencies = [
            {'left': ('A', 'B'), 'right': ('C',)},
            {'left': ('B', 'C'), 'right': ('A', 'D')},
            {'left': ('D',), 'right': ('E',)},
            {'left': ('C', 'F'), 'right': ('B',)},
        ]
        self.assertEqual(closure(attributes, dependencies), {'D', 'E'})

    def test_advanced(self):
        attributes = {'A', 'B'}
        dependencies = [
            {'left': ('A', 'B'), 'right': ('C',)},
            {'left': ('B', 'C'), 'right': ('A', 'D')},
            {'left': ('D',), 'right': ('E',)},
            {'left': ('C', 'F'), 'right': ('B',)},
        ]
        self.assertEqual(closure(attributes, dependencies),
                         {'A', 'B', 'C', 'D', 'E'})


class TestPrioritizedRelations(TestCase):
    def test_all_low_priority(self):
        """
        Check the case when no base relations and dependencies are provided
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}, 'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E', 'F'}, 'pk': {'E'}}

        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r1, r2, r3],
            base_relations=[],
            dependencies=[],
            all_attributes={'A', 'B', 'C', 'D', 'E', 'F'}))
        self.assertEqual(priorities, (1,) * 3)

    def test_all_low_priority_with_base_relations(self):
        """
        Check the case when some base relations are provided but priorities are
        still lowest for all relations
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}, 'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E', 'F'}, 'pk': {'E'}}

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
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'E'}, 'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'B', 'C'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E', 'F'}, 'pk': {'E'}}

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
            {'left': ('A', 'B'), 'right': ('C',)},
            {'left': ('B', 'C'), 'right': ('A', 'D')},
            {'left': ('D',), 'right': ('E',)}
        ]

        # case 1: overwhelm in attributes
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
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'C'}, 'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'D', 'E'}, 'pk': {'D'}}
        base = {'name': 'R4', 'attributes': {'C', 'A'}, 'pk': {'D'}}
        dependencies = [
            {'left': ('A', 'B'), 'right': ('C',)},
            {'left': ('B', 'C'), 'right': ('A', 'D')},
            {'left': ('D',), 'right': ('E',)}
        ]
        all_attributes = {'A', 'B', 'C', 'D', 'E'}

        relations_to_check, priorities = zip(*prioritized_relations(
            relations=[r1, r2, r3],
            base_relations=[base],
            dependencies=dependencies,
            all_attributes=all_attributes))
        self.assertEqual(priorities, (3, 2, 1))
