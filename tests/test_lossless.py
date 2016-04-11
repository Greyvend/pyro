from unittest import TestCase

from pyro.lossless import _build_tableau, is_lossless


class TestBuildTableau(TestCase):
    def test_all_attributes_in_all_relations(self):
        """
        Check the case when both tables have same set of attributes (in fact,
        it should be one table)
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT',
                                           'D': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT',
                                           'D': 'INT'}}
        tableau = _build_tableau([r1, r2])
        # all values of this table should be equal to the keys as all
        # attributes are present in both tables
        for row in tableau:
            for k, v in row.iteritems():
                self.assertEqual((k,), v)

    def test_all_different_attributes(self):
        """
        Check the case when tables have all attributes different, no
        intersection
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'}}
        tableau = _build_tableau([r1, r2])
        self.assertEqual(tableau[0]['A'], ('A',))
        self.assertEqual(tableau[0]['B'], ('B',))
        self.assertEqual(tableau[0]['C'], ('C', 'R1'))
        self.assertEqual(tableau[0]['D'], ('D', 'R1'))
        self.assertEqual(tableau[1]['A'], ('A', 'R2'))
        self.assertEqual(tableau[1]['B'], ('B', 'R2'))
        self.assertEqual(tableau[1]['C'], ('C',))
        self.assertEqual(tableau[1]['D'], ('D',))

    def test_intersecting_attributes(self):
        """
        Check the case when tables have some attributes in common
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'B': 'INT', 'C': 'INT', 'D': 'INT'}}
        tableau = _build_tableau([r1, r2])
        self.assertEqual(tableau[0]['A'], ('A',))
        self.assertEqual(tableau[0]['B'], ('B',))
        self.assertEqual(tableau[0]['C'], ('C',))
        self.assertEqual(tableau[0]['D'], ('D', 'R1'))
        self.assertEqual(tableau[1]['A'], ('A', 'R2'))
        self.assertEqual(tableau[1]['B'], ('B',))
        self.assertEqual(tableau[1]['C'], ('C',))
        self.assertEqual(tableau[1]['D'], ('D',))


class TestIsLossless(TestCase):
    def test_single_relation_no_deps(self):
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        deps = []
        self.assertTrue(is_lossless([r1], deps))

    def test_single_relation(self):
        r1 = {'name': 'R1', 'attributes': {'pk': 'INT', 'A_1': 'INT',
                                           'A_2': 'INT'}}
        deps = [{'left': {'pk'}, 'right': {'A_1', 'A_2'}}]
        self.assertTrue(is_lossless([r1], deps))

    def test_cartesian_product_no_deps(self):
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'}}
        deps = []
        self.assertFalse(is_lossless([r1, r2], deps))

    def test_cartesian_product_separate_deps(self):
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'}}
        deps = [{'left': ('A',), 'right': ('B',)},
                {'left': ('C',), 'right': ('D',)}]
        self.assertFalse(is_lossless([r1, r2], deps))

    def test_simplest_case(self):
        """
        Test chase algorithm in the simplest succeeding case with single
        dependency held on same attribute value
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'C': 'INT', 'D': 'INT'}}
        deps = [{'left': ('C',), 'right': ('D',)}]
        self.assertTrue(is_lossless([r1, r2], deps))

    def test_advanced(self):
        """
        This example was taken from the Ullman's "Database Systems - The
        Complete book"
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'D': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'A': 'INT', 'C': 'INT'}}
        r3 = {'name': 'R3', 'attributes': {'B': 'INT', 'C': 'INT', 'D': 'INT'}}
        deps = [
            {'left': ('A',), 'right': ('B',)},
            {'left': ('B',), 'right': ('C',)},
            {'left': ('C', 'D'), 'right': ('A',)},
        ]
        self.assertTrue(is_lossless([r1, r2, r3], deps))

    def test_advanced_lossless_not_held(self):
        """
        This example was also taken from the Ullman's "Database Systems - The
        Complete book"
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'B': 'INT', 'C': 'INT'}}
        r3 = {'name': 'R3', 'attributes': {'C': 'INT', 'D': 'INT'}}
        deps = [
            {'left': ('B',), 'right': ('A', 'D')}
        ]
        self.assertFalse(is_lossless([r1, r2, r3], deps))

    def test_separate_deps_common_attributes(self):
        """
        Check the case when relations intersect over some attributes but don't
        have any dependencies uniting them
        """
        r1 = {'name': 'R1', 'attributes': {'A': 'INT', 'B': 'INT', 'C': 'INT'}}
        r2 = {'name': 'R2', 'attributes': {'B': 'INT', 'D': 'INT', 'E': 'INT'}}
        deps = [
            {'left': ('A', 'B'), 'right': ('C',)},  # R1 primary key
            {'left': ('B', 'D'), 'right': ('E',)},  # R2 primary key
        ]
        self.assertFalse(is_lossless([r1, r2], deps))
