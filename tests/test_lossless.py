from unittest import TestCase

from pyro.lossless import _build_tableau, is_lossless, min_dict


class TestBuildTableau(TestCase):
    def test_all_attributes_in_all_relations(self):
        """
        Check the case when both tables have same set of attributes (in fact,
        it should be one table)
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'C', 'D'}}
        r2 = {'name': 'R2', 'attributes': {'A', 'B', 'C', 'D'}}
        tableau = _build_tableau([r1, r2])
        # all values of this table should be equal to the keys as all
        # attributes are present in both tables
        for row in tableau:
            for k, v in row.iteritems():
                self.assertEqual(k, v)

    def test_all_different_attributes(self):
        """
        Check the case when tables have all attributes different, no
        intersection
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}}
        tableau = _build_tableau([r1, r2])
        self.assertEqual(tableau[0]['A'], 'A')
        self.assertEqual(tableau[0]['B'], 'B')
        self.assertEqual(tableau[0]['C'], 'C_R1')
        self.assertEqual(tableau[0]['D'], 'D_R1')
        self.assertEqual(tableau[1]['A'], 'A_R2')
        self.assertEqual(tableau[1]['B'], 'B_R2')
        self.assertEqual(tableau[1]['C'], 'C')
        self.assertEqual(tableau[1]['D'], 'D')

    def test_intersecting_attributes(self):
        """
        Check the case when tables have some attributes in common
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'C'}}
        r2 = {'name': 'R2', 'attributes': {'B', 'C', 'D'}}
        tableau = _build_tableau([r1, r2])
        self.assertEqual(tableau[0]['A'], 'A')
        self.assertEqual(tableau[0]['B'], 'B')
        self.assertEqual(tableau[0]['C'], 'C')
        self.assertEqual(tableau[0]['D'], 'D_R1')
        self.assertEqual(tableau[1]['A'], 'A_R2')
        self.assertEqual(tableau[1]['B'], 'B')
        self.assertEqual(tableau[1]['C'], 'C')
        self.assertEqual(tableau[1]['D'], 'D')


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


class TestIsLossless(TestCase):
    def test_cartesian_product_no_deps(self):
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}}
        deps = []
        self.assertFalse(is_lossless([r1, r2], deps))

    def test_cartesian_product_separate_deps(self):
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}}
        deps = [{'left': ('A',), 'right': ('B',)},
                {'left': ('C',), 'right': ('D',)}]
        self.assertFalse(is_lossless([r1, r2], deps))

    def test_simplest_case(self):
        """
        Test chase algorithm in the simplest succeeding case with single
        dependency held on same attribute value
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'C'}}
        r2 = {'name': 'R2', 'attributes': {'C', 'D'}}
        deps = [{'left': ('C',), 'right': ('D',)}]
        self.assertTrue(is_lossless([r1, r2], deps))

    def test_advanced(self):
        """
        This example was taken from the Ullman's "Database Systems - The
        Complete book"
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'D'}}
        r2 = {'name': 'R2', 'attributes': {'A', 'C'}}
        r3 = {'name': 'R3', 'attributes': {'B', 'C', 'D'}}
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
        r1 = {'name': 'R1', 'attributes': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'B', 'C'}}
        r3 = {'name': 'R3', 'attributes': {'C', 'D'}}
        deps = [
            {'left': ('B',), 'right': ('A', 'D')}
        ]
        self.assertFalse(is_lossless([r1, r2, r3], deps))

    def test_separate_deps_common_attributes(self):
        """
        Check the case when relations intersect over some attributes but don't
        have any dependencies uniting them
        """
        r1 = {'name': 'R1', 'attributes': {'A', 'B', 'C'}}
        r2 = {'name': 'R2', 'attributes': {'B', 'D', 'E'}}
        deps = [
            {'left': ('A', 'B'), 'right': ('C',)},  # R1 primary key
            {'left': ('B', 'D'), 'right': ('E',)},  # R2 primary key
        ]
        self.assertFalse(is_lossless([r1, r2], deps))
