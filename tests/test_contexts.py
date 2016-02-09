from unittest import TestCase

from pyro.contexts import closure


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
