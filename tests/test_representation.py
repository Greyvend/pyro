from unittest import TestCase
from unittest.mock import patch

# noinspection PyProtectedMember
from pyro.representation import _get_hierarchy


class TestCountAttributes(TestCase):
    @patch('pyro.representation.db')
    def test_single_attribute(self, mock_db):
        """
        Check the case when hierarchy has correct order of attributes,
        increasing amounts of values
        """
        relation_name = 'TJ_random'
        dimension = {'attributes': ['R1.A1']}
        mock_db.count_attributes.return_value = [55]

        hierarchy = _get_hierarchy(None, relation_name, dimension)

        self.assertEqual(hierarchy, ('A1',))

    @patch('pyro.representation.db')
    def test_ascending_order(self, mock_db):
        """
        Check the case when hierarchy has correct order of attributes,
        increasing amounts of values
        """
        relation_name = 'TJ_random'
        dimension = {'attributes': ['R1.A1', 'R2.A5', 'R1.A4']}
        mock_db.count_attributes.return_value = [55, 64, 1898]

        hierarchy = _get_hierarchy(None, relation_name, dimension)

        self.assertEqual(hierarchy, ('A1', 'A5', 'A4'))

    @patch('pyro.representation.db')
    def test_random_order(self, mock_db):
        """
        Check the case when hierarchy has different order of attributes
        """
        relation_name = 'TJ_random'
        dimension = {'attributes': ['R1.A1', 'R2.A5', 'R1.A4']}
        mock_db.count_attributes.return_value = [55, 1000, 64]

        hierarchy = _get_hierarchy(None, relation_name, dimension)

        self.assertEqual(hierarchy, ('A1', 'A4', 'A5'))
