from unittest import TestCase

from pyro.utils import containing_relation


class TestContainingRelation(TestCase):
    def test_no_containing_relation(self):
        attribute = 'G'
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}, 'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E', 'F'}, 'pk': {'E'}}
        relations = [r1, r2, r3]

        self.assertRaises(ValueError, containing_relation, relations,
                          attribute)

    def test_one_containing_relation(self):
        attribute = 'D'
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}, 'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}, 'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E', 'F'}, 'pk': {'E'}}
        relations = [r1, r2, r3]

        self.assertDictEqual(containing_relation(relations, attribute), r2)

    def test_two_containing_relations(self):
        attribute = 'B'
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}, 'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'B', 'D'}, 'pk': {'B'}}
        r3 = {'name': 'R3', 'attributes': {'E', 'F'}, 'pk': {'E'}}

        self.assertDictEqual(containing_relation([r1, r2, r3], attribute), r1)
        self.assertDictEqual(containing_relation([r2, r1, r3], attribute), r2)

