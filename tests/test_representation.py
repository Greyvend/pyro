from unittest import TestCase
from unittest.mock import patch

from pyro.representation import _get_hierarchy, _to_html, _group_cells


class TestGetHierarchy(TestCase):
    @patch('pyro.representation.db')
    def test_single_attribute(self, mock_db):
        """
        Check the case when hierarchy has correct order of attributes,
        increasing amounts of values
        """
        relation_name = 'TJ_random'
        attributes = ['A1']
        mock_db.count_attributes.return_value = [55]

        hierarchy = _get_hierarchy(None, relation_name, attributes)

        self.assertEqual(hierarchy, ('A1',))

    @patch('pyro.representation.db')
    def test_ascending_order(self, mock_db):
        """
        Check the case when hierarchy has correct order of attributes,
        increasing amounts of values
        """
        relation_name = 'TJ_random'
        attributes = ['A1', 'A5', 'A4']
        mock_db.count_attributes.return_value = [55, 64, 1898]

        hierarchy = _get_hierarchy(None, relation_name, attributes)

        self.assertEqual(hierarchy, ('A1', 'A5', 'A4'))

    @patch('pyro.representation.db')
    def test_random_order(self, mock_db):
        """
        Check the case when hierarchy has different order of attributes
        """
        relation_name = 'TJ_random'
        attributes = ['A1', 'A5', 'A4']
        mock_db.count_attributes.return_value = [55, 1000, 64]

        hierarchy = _get_hierarchy(None, relation_name, attributes)

        self.assertEqual(hierarchy, ('A1', 'A4', 'A5'))


class TestGroupCells(TestCase):
    def test_general(self):
        table = [[None, 'value_11', 'value_12', (1, 2, 3, 4)],
                 [None, 'value_21', 'value_22', (1, 12, 3, 4)],
                 [None, 'value_21', 'value_32', (1, 2, 13, 4)],
                 ['a', 'value_11', 'value_12', (1, 2, 3, 14)],
                 ['a', 'value_21', 'value_22', (1, 2, 23, 4)],
                 ['a', 'value_21', 'value_32', (1, 22, 3, 4)]]

        cell_groups = _group_cells(table, max_col=3)

        self.assertEqual(len(cell_groups), 3)
        self.assertDictEqual(cell_groups[0], {None: 3, 'a': 3})
        self.assertDictEqual(cell_groups[1], {'value_11': 1, 'value_21': 2})
        self.assertDictEqual(cell_groups[2], {'value_12': 1, 'value_22': 1,
                                              'value_32': 1})

    def test_max_col_borders(self):
        table = [[None, 'value_11', 'value_12', (1, 2, 3, 4)],
                 [None, 'value_21', 'value_22', (1, 12, 3, 4)],
                 [None, 'value_21', 'value_32', (1, 2, 13, 4)],
                 ['a', 'value_11', 'value_12', (1, 2, 3, 14)],
                 ['a', 'value_21', 'value_22', (1, 2, 23, 4)],
                 ['a', 'value_21', 'value_32', (1, 22, 3, 4)]]

        cell_groups = _group_cells(table, max_col=1)

        self.assertEqual(len(cell_groups), 1)
        self.assertDictEqual(cell_groups[0], {None: 3, 'a': 3})

        cell_groups = _group_cells(table, max_col=4)

        self.assertEqual(len(cell_groups), 4)
        self.assertIn((1, 2, 3, 4), cell_groups[3])


class TestToHtml(TestCase):
    def test_no_spans(self):
        """
        Check the case when there are no spanning cells in the table
        """
        table = [['', 'Y1', 'value_1', 'value_2'],
                 ['', 'Y2', 'value_2_1', 'value_2_2'],
                 ['X1', '', 'measure', 'measure'],
                 ['X1_value_1', '', (11, 12.5, 13), (14.6,)],
                 ['X1_value_2', '', (41.0, 52, 65.2), (111.2, 3)],
                 ['X1_value_3', '', (84, 55.3, 7), (10, 3.5, 93)],]
        dimensions = [['Y1', 'Y2'], ['X1']]

        html = _to_html(table, dimensions)

        self.assertNotIn('colspan', html)
        self.assertNotIn('rowspan', html)
        self.assertNotIn('(', html)
        self.assertNotIn(')', html)
        self.assertEqual(html.count('<table>'), 1)
        self.assertEqual(html.count('</table>'), 1)
        self.assertEqual(html.count('<tr>'), 6)
        self.assertEqual(html.count('</tr>'), 6)
        self.assertEqual(html.count('<th'), 3)
        self.assertEqual(html.count('</th>'), 3)
        self.assertEqual(html.count('<td'), 6 * 4 - 3)
        self.assertEqual(html.count('</td>'), 6 * 4 - 3)

    def test_col_spans(self):
        """
        Check the case with column spanning cells
        """
        table = [['', 'Y1', 'v_1', 'v_1', 'v_2', 'v_2', 'v_3'],
                 ['', 'Y2', 'v_21', 'v_22', 'v_21', 'v_22', 'v_21'],
                 ['X1', '', 'measure', 'measure', 'measure', 'measure',
                  'measure'],
                 ['X1_value_1', '', 11, 12, 13, 14, 15],]
        dimensions = [['Y1', 'Y2'], ['X1']]

        html = _to_html(table, dimensions)

        self.assertEqual(html.count('colspan'), 2)
        self.assertNotIn('rowspan', html)
        self.assertNotIn('(', html)
        self.assertNotIn(')', html)
        self.assertEqual(html.count('<table>'), 1)
        self.assertEqual(html.count('</table>'), 1)
        self.assertEqual(html.count('<tr'), 4)
        self.assertEqual(html.count('</tr>'), 4)
        self.assertEqual(html.count('<th'), 3)
        self.assertEqual(html.count('</th'), 3)
        self.assertEqual(html.count('<td'), 7 * 4 - 2 - 3)
        self.assertEqual(html.count('</td>'), 7 * 4 - 2 - 3)

    def test_row_spans(self):
        """
        Check the case with row spanning cells
        """
        table = [['', '', 'Y1', 'value_1', 'value_2'],
                 ['', '', 'Y2', 'value_2_1', 'value_2_2'],
                 ['X1', 'X2', '', 'measure', 'measure'],
                 ['X1_v1', 'X2_v1', '', (11, 12.5, 13), (14.6,)],
                 ['X1_v1', 'X2_v2', '', (41.0, 52, 65.2), (111.2, 3)],
                 ['X1_v2', 'X2_v1', '', (4.0, 52, 65.2), (111.2, 3)],
                 ['X1_v2', 'X2_v2', '', (51.0, 56, 65.2), (132.2, 3)],
                 ['X1_v2', 'X2_v3', '', (41.0, 52, 65.2), (111.2, 3)],
                 ['X1_v3', 'X2_v1', '', (84, 55.3, 7), (10, 3.5, 93)],]
        dimensions = [['Y1', 'Y2'], ['X1', 'X2']]

        html = _to_html(table, dimensions)

        self.assertEqual(html.count('rowspan'), 2)
        self.assertEqual(html.count('colspan'), 2)  # in first empty cells
        self.assertNotIn('(', html)
        self.assertNotIn(')', html)
        self.assertEqual(html.count('<table>'), 1)
        self.assertEqual(html.count('</table>'), 1)
        self.assertEqual(html.count('<tr>'), 9)
        self.assertEqual(html.count('</tr>'), 9)
        self.assertEqual(html.count('<th'), 4)
        self.assertEqual(html.count('</th>'), 4)
        self.assertEqual(html.count('<td'), 9 * 5 - 2 - 1 - 2 - 4)
        self.assertEqual(html.count('</td>'), 9 * 5 - 2 - 1 - 2 - 4)
