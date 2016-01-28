from unittest import TestCase

from pyro.contexts import is_lossless


class TestIsLossless(TestCase):
    def test_cartesian_product_no_deps(self):
        r1 = {'attributes': ['A', 'B']}
        r2 = {'attributes': ['C', 'D']}
        deps = []
        self.assertFalse(is_lossless([r1, r2], deps))

    def test_cartesian_product_separate_deps(self):
        r1 = {'attributes': ['A', 'B']}
        r2 = {'attributes': ['C', 'D']}
        deps = [{'A': 'B'}, {'C': 'D'}]
        self.assertFalse(is_lossless([r1, r2], deps))

    def test_intersecting_deps(self):
        """
        Test chase algorithm in the case when dependencies cross relations
        """
        r1 = {'attributes': ['A', 'B']}
        r2 = {'attributes': ['C', 'D']}
        deps = [{'A': 'C'}]
        self.assertTrue(is_lossless([r1, r2], deps))
