from unittest import TestCase

from pyro.constraints.operations import project, is_domain_included, \
    _is_predicate_domain_included


class TestProject(TestCase):
    def test_various(self):
        constraint = [
            [{'attribute': 'A1', 'operation': '>', 'value': 4}],
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179}],
            [{'attribute': 'A2', 'operation': '<>', 'value': 16},
             {'attribute': 'A3', 'operation': '<=', 'value': 13}]
        ]
        attributes = ['A2', 'A3']

        projection = project(constraint, attributes)

        self.assertEqual(len(projection), 2)  # first sublist should be gone
        self.assertEqual(projection[0], [constraint[1][1]])
        self.assertEqual(projection[1], constraint[2])

    def test_empty(self):
        constraint = [
            [{'attribute': 'A1', 'operation': '>', 'value': 4}],
            [{'attribute': 'A1', 'operation': '=', 'value': 3}]
        ]
        attributes = ['A2', 'A3']

        projection = project(constraint, attributes)

        self.assertEqual(len(projection), 0)
        self.assertEqual(projection, [])


class TestIsPredicateDomainIncluded(TestCase):
    """
    Some corner cases for single predicates
    """
    def test_between_predicates(self):
        p1 = {'attribute': 'A1', 'operation': 'BETWEEN', 'value': [3, 15]}
        p2 = {'attribute': 'A1', 'operation': 'BETWEEN', 'value': [2, 11]}

        self.assertFalse(_is_predicate_domain_included(p1, p2))
        self.assertFalse(_is_predicate_domain_included(p2, p1))


class TestIsDomainIncluded(TestCase):
    def test_empty_constraint(self):
        c1 = [[{'attribute': 'A1', 'operation': '=', 'value': 3}]]
        c2 = []

        self.assertTrue(is_domain_included(c1, c2))
        self.assertFalse(is_domain_included(c2, c1))

    def test_empty_both_constraints(self):
        c1 = []
        c2 = []

        self.assertTrue(is_domain_included(c1, c2))
        self.assertTrue(is_domain_included(c2, c1))

    def test_single_conjunction_clause_successful(self):
        c1 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179},
             {'attribute': 'A3', 'operation': '<=', 'value': 13}]
        ]
        c2 = [
            [{'attribute': 'A1', 'operation': 'IN', 'value': [3, 5, 6]},
             {'attribute': 'A2', 'operation': '<=', 'value': 179}]
        ]

        self.assertTrue(is_domain_included(c1, c2))
        self.assertFalse(is_domain_included(c2, c1))

    def test_single_conjunction_clause_different_attributes(self):
        c1 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179}]
        ]
        c2 = [
            [{'attribute': 'A2', 'operation': '<>', 'value': 16},
             {'attribute': 'A3', 'operation': '<=', 'value': 13}]
        ]

        self.assertFalse(is_domain_included(c1, c2))
        self.assertFalse(is_domain_included(c2, c1))

    def test_single_conjunction_clause_direct_subset(self):
        c1 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179}]
        ]
        c2 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179},
             {'attribute': 'A3', 'operation': '<=', 'value': 13}]
        ]

        self.assertFalse(is_domain_included(c1, c2))
        self.assertTrue(is_domain_included(c2, c1))

    def test_multiple_conjunction_clauses_partially_included(self):
        c1 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3}],
            [{'attribute': 'A2', 'operation': 'BETWEEN', 'value': [3, 15]}],
        ]
        c2 = [
            [{'attribute': 'A3', 'operation': 'LIKE', 'value': '%sfdabc%'}],
            [{'attribute': 'A2', 'operation': 'BETWEEN', 'value': [6, 11]}],
            [{'attribute': 'A1', 'operation': '>', 'value': 0}]
        ]

        self.assertFalse(is_domain_included(c1, c2))
        self.assertFalse(is_domain_included(c2, c1))

    def test_multiple_conjunction_clauses_included(self):
        c1 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179}],
            [{'attribute': 'A4', 'operation': 'BETWEEN', 'value': [3, 15]},
             {'attribute': 'A3', 'operation': 'LIKE', 'value': 'abc%'}],
        ]
        c2 = [
            [{'attribute': 'A3', 'operation': 'LIKE', 'value': '%sfdabc%'}],
            [{'attribute': 'A4', 'operation': 'BETWEEN', 'value': [2, 17]},
             {'attribute': 'A3', 'operation': 'LIKE', 'value': 'abc%'}],
            [{'attribute': 'A1', 'operation': '>', 'value': 0}]
        ]

        self.assertTrue(is_domain_included(c1, c2))
        self.assertFalse(is_domain_included(c2, c1))

    def test_multiple_conjunction_clauses_not_included(self):
        """
        First conjunction clause's domain is a subset of first one in second
        constraint but second clause doesn't have any corresponding clause.
        """
        c1 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179}],
            [{'attribute': 'A1', 'operation': 'BETWEEN', 'value': [3, 15]},
             {'attribute': 'A3', 'operation': 'LIKE', 'value': 'abc%'}],
        ]
        c2 = [
            [{'attribute': 'A1', 'operation': '=', 'value': 3},
             {'attribute': 'A2', 'operation': '<', 'value': 179}]
        ]

        self.assertFalse(is_domain_included(c1, c2))
        self.assertTrue(is_domain_included(c2, c1))
