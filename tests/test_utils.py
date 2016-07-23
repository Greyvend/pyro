from unittest import TestCase

from pyro.utils import containing_relation, min_dict, all_equal, all_attributes


class TestAllAttributes(TestCase):
    def test_no_intersecting_attributes(self):
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'},
              'pk': {'E'}}
        relations = [r1, r2, r3]

        attributes = all_attributes(relations)
        self.assertEqual(attributes, {'A': 'INT', 'B': 'INT', 'C': 'INT',
                                      'D': 'INT', 'E': 'INT', 'F': 'INT'})

    def test_intersecting_attributes(self):
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'A': 'INT', 'C': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'B': 'INT', 'C': 'INT'},
              'pk': {'B'}}
        relations = [r1, r2, r3]

        attributes = all_attributes(relations)
        self.assertEqual(attributes, {'A': 'INT', 'B': 'INT', 'C': 'INT'})


class TestContainingRelation(TestCase):
    def test_no_containing_relation(self):
        attribute = 'G'
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'},
              'pk': {'E'}}
        relations = [r1, r2, r3]

        self.assertRaises(ValueError, containing_relation, relations,
                          attribute)

    def test_one_containing_relation(self):
        attribute_name = 'DD'
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'DD': 'INT'},
              'pk': {'C'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'},
              'pk': {'E'}}
        relations = [r1, r2, r3]

        self.assertDictEqual(containing_relation(relations, attribute_name),
                             r2)

    def test_two_containing_relations(self):
        attribute_name = 'Bob'
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'Bob': 'INT'},
              'pk': {'A'}}
        r2 = {'name': 'R2', 'attributes': {'Bob': 'INT', 'D': 'INT'},
              'pk': {'Bob'}}
        r3 = {'name': 'R3', 'attributes': {'E': 'INT', 'F': 'INT'},
              'pk': {'E'}}

        self.assertDictEqual(containing_relation([r1, r2, r3], attribute_name),
                             r1)
        self.assertDictEqual(containing_relation([r2, r1, r3], attribute_name),
                             r2)


class TestMinDict(TestCase):
    def test_same(self):
        """
        Check that calling the routine on dicts with same key/values in
        different order results in either of those dicts
        """
        d1 = {'A': ('A',), 'B': ('B', 3)}
        d2 = {'B': ('B', 3), 'A': ('A',)}
        self.assertEqual(min_dict(d1, d2), d1)
        self.assertEqual(min_dict(d1, d2), d2)

    def test_different_keys(self):
        d1 = {'A': ('A',), 'B': ('B', 3)}
        d2 = {'C': ('C', 1), 'D': ('D',)}
        self.assertEqual(min_dict(d1, d2), {})

    def test_intersecting_keys(self):
        d1 = {'A': ('A',), 'B': ('B', 3), 'C': ('C', 1)}
        d2 = {'A': ('A', 1), 'B': ('B', 2), 'D': ('D', 3)}
        self.assertEqual(min_dict(d1, d2), {'A': ('A',), 'B': ('B', 2)})


class TestAllEqual(TestCase):
    def test_single_value(self):
        """
        Check the case when only one value is supplied
        """
        values = [{'key': 'value'}]
        self.assertTrue(all_equal(values))

    def test_success(self):
        """
        Check the case with several equal values
        """
        values = [{'key': 'value'}, {'key': 'value'}]
        self.assertTrue(all_equal(values))

    def test_failure(self):
        """
        Check the case with several different values
        """
        values = [{'key': 'value'}, {'key': 'another_value'}]
        self.assertFalse(all_equal(values))
