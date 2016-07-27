from unittest import TestCase

import pyro.tj


class TestVectorSerialization(TestCase):
    def test_encode_vector(self):
        pyro.tj.VECTOR_SEPARATOR = ','
        self.assertEqual(pyro.tj.encode_vector([{"name": "users"},
                                                {"name": "addresses"},
                                                {"name": "payments"},
                                                {"name": "pictures"}]),
                         'users,addresses,payments,pictures')
        pyro.tj.VECTOR_SEPARATOR = '/'
        self.assertEqual(pyro.tj.encode_vector([{"name": "users"},
                                                {"name": "addresses"},
                                                {"name": "payments"},
                                                {"name": "pictures"}]),
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
