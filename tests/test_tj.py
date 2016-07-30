from unittest import TestCase

import pyro.tj


class TestVectorSerialization(TestCase):
    def test_encode_vector(self):
        pyro.tj.VECTOR_SEPARATOR = ','
        self.assertEqual(pyro.tj.encode_vector([{'name': 'users'},
                                                {'name': 'addresses'},
                                                {'name': 'payments'},
                                                {'name': 'pictures'}]),
                         'users,addresses,payments,pictures')
        pyro.tj.VECTOR_SEPARATOR = '/'
        self.assertEqual(pyro.tj.encode_vector([{'name': 'users'},
                                                {'name': 'addresses'},
                                                {'name': 'payments'},
                                                {'name': 'pictures'}]),
                         'users/addresses/payments/pictures')

    def test_decode_vector(self):
        pyro.tj.VECTOR_SEPARATOR = ','
        pyro.tj.VECTOR_ATTRIBUTE = 'g'
        self.assertEqual(pyro.tj.decode_vector(
            {'g': 'users,addresses,payments,pictures'}),
            ['users', 'addresses', 'payments', 'pictures'])


class TestIsLessOrEqual(TestCase):
    def test_same_relation_set(self):
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'second', 'third'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'third', 'second'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['third', 'second', 'first'],
                                                 ['first', 'second', 'third']))

    def test_different_relation_set(self):
        # case 1: 1 out of 3 relations
        self.assertTrue(pyro.tj.is_less_or_equal(['first'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['third'],
                                                 ['first', 'second', 'third']))
        # case 2: 2 out of 3 relations
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'second'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'third'],
                                                 ['first', 'second', 'third']))
        # case 3: different relations
        self.assertFalse(pyro.tj.is_less_or_equal(
            ['first', 'second', 'fourth'], ['first', 'second', 'third']))
        self.assertFalse(pyro.tj.is_less_or_equal(['fourth'], ['fifth']))
        # case 4: excess relations
        self.assertFalse(pyro.tj.is_less_or_equal(
            ['first', 'second', 'third', 'fourth'],
            ['first', 'second', 'third']))


class TestIsSubordinate(TestCase):
    def test_same_relation_set(self):
        # case 1: single relation in context
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}}]
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'aa', 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': None, 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': None, 'g': 'R_1'}))

        # case 2: two relations in context
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}}]
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'bb', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'bb', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'}))

    def test_different_relation_sets(self):
        # case 1: different attribute values
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}}]
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'},
            {'A_1': None, 'A_2': 'b', 'A_3': 'c', 'g': 'R_2'}))
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1,R_2'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'}))

        # case 2: same attribute values but different relation set, which leads
        # to non subordination
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_1': 'INT', 'A_3': 'INT'}},
                   {'name': 'R_3', 'attributes': {'A_2': 'INT', 'A_4': 'INT'}}]
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'A_4': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'A_4': 'd', 'g': 'R_2,R_3'})
        )
