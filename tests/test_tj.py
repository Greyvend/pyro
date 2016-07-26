from unittest import TestCase

from pyro.tj import is_less_or_equal


class TestCmpVectors(TestCase):
    def test_same_relation_set(self):
        self.assertTrue(is_less_or_equal(['first', 'second', 'third'],
                                         ['first', 'second', 'third']))
        self.assertTrue(is_less_or_equal(['first', 'third', 'second'],
                                         ['first', 'second', 'third']))
        self.assertTrue(is_less_or_equal(['third', 'second', 'first'],
                                         ['first', 'second', 'third']))

    def test_different_relation_set(self):
        # case 1: 1 out of 3 relations
        self.assertTrue(is_less_or_equal(['first'],
                                         ['first', 'second', 'third']))
        self.assertTrue(is_less_or_equal(['third'],
                                         ['first', 'second', 'third']))
        # case 2: 2 out of 3 relations
        self.assertTrue(is_less_or_equal(['first', 'second'],
                                         ['first', 'second', 'third']))
        self.assertTrue(is_less_or_equal(['first', 'third'],
                                         ['first', 'second', 'third']))
        # case 3: different relations
        self.assertFalse(is_less_or_equal(['first', 'second', 'fourth'],
                                          ['first', 'second', 'third']))
        self.assertFalse(is_less_or_equal(['fourth'], ['fifth']))
        # case 4: excess relations
        self.assertFalse(is_less_or_equal(['first', 'second', 'third',
                                           'fourth'],
                                          ['first', 'second', 'third']))
