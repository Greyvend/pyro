from datetime import datetime
from unittest import TestCase

from pyro.utils import containing_relation, min_dict, all_equal, \
    all_attributes, chunks, assemble_list, xstr


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


class TestChunks(TestCase):
    def test_divisible(self):
        """
        Check behavior when whole list is factored by chunks
        """
        l = range(30)
        n = 15
        _chunks = chunks(l, n)
        self.assertEqual(next(_chunks), range(15))
        self.assertEqual(next(_chunks), range(15, 30))
        self.assertRaises(StopIteration, next, _chunks)

    def test_non_divisible(self):
        """
        Check behavior when whole list can't be factored by chunks
        """
        l = range(31)
        n = 15
        _chunks = chunks(l, n)
        self.assertEqual(next(_chunks), range(15))
        self.assertEqual(next(_chunks), range(15, 30))
        self.assertEqual(next(_chunks), range(30, 31))
        self.assertRaises(StopIteration, next, _chunks)

    def test_with_list(self):
        """
        Check functionality with list object
        """
        l = ['a', 'b', 'c', 'd', 'e']
        n = 2
        _chunks = chunks(l, n)
        self.assertEqual(next(_chunks), ['a', 'b'])
        self.assertEqual(next(_chunks), ['c', 'd'])
        self.assertEqual(next(_chunks), ['e'])
        self.assertRaises(StopIteration, next, _chunks)


class TestAssembleList(TestCase):
    def test_mixed(self):
        """
        Check behavior when lists share common list elements
        """
        l = [[{'name': 'R1'}, {'name': 'R2'}, {'name': 'R3'}],
             [{'name': 'R2'}, {'name': 'R4'}, {'name': 'R5'}]]

        new_list = assemble_list(l, key=lambda r: r['name'])

        self.assertEqual(len(new_list), 5)
        self.assertEqual(new_list[0], {'name': 'R1'})
        self.assertEqual(new_list[1], {'name': 'R2'})
        self.assertEqual(new_list[2], {'name': 'R3'})
        self.assertEqual(new_list[3], {'name': 'R4'})
        self.assertEqual(new_list[4], {'name': 'R5'})

    def test_different(self):
        """
        Check behavior when each list has different items in sublist
        """
        l = [[{'name': 'R1'}, {'name': 'R2'}, {'name': 'R3'}],
             [{'name': 'R4'}, {'name': 'R5'}]]

        new_list = assemble_list(l, key=lambda r: r['name'])

        self.assertEqual(len(new_list), 5)
        self.assertEqual(new_list[0], {'name': 'R1'})
        self.assertEqual(new_list[1], {'name': 'R2'})
        self.assertEqual(new_list[2], {'name': 'R3'})
        self.assertEqual(new_list[3], {'name': 'R4'})
        self.assertEqual(new_list[4], {'name': 'R5'})

    def test_empty(self):
        """
        Check behavior when one of the lists is empty
        """
        l = [[], [{'name': 'R4'}, {'name': 'R5'}]]

        new_list = assemble_list(l, key=lambda r: r['name'])

        self.assertEqual(len(new_list), 2)
        self.assertEqual(new_list[0], {'name': 'R4'})
        self.assertEqual(new_list[1], {'name': 'R5'})


class TestXstr(TestCase):
    def test_tuple_without_none(self):
        t = (20.4, 4, 55)

        str_t = xstr(t)

        self.assertEqual(str_t, '20.4, 4, 55')

    def test_tuple_with_none(self):
        t = (20.4, None, 55, None, None)

        str_t = xstr(t)

        self.assertEqual(str_t, '20.4, 55')

    def test_tuple_only_nones(self):
        t = (None, None)

        str_t = xstr(t)

        self.assertEqual(str_t, '')

    def test_tuple_single_none(self):
        t = (None,)

        str_t = xstr(t)

        self.assertEqual(str_t, '')

    def test_tuple_single(self):
        t = (22,)

        str_t = xstr(t)

        self.assertEqual(str_t, '22')

    def test_tuple_types_mix(self):
        t = (22, True, datetime(2014, 7, 15, 16), 'str here')

        str_t = xstr(t)

        self.assertEqual(str_t, '22, True, 2014-07-15 16:00:00, str here')

    def test_number(self):
        obj = 15.3

        str_t = xstr(obj)

        self.assertEqual(str_t, '15.3')

    def test_str(self):
        obj = 'string it is'

        str_t = xstr(obj)

        self.assertEqual(str_t, 'string it is')
