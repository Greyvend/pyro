from unittest import TestCase

from pyro.constraints import project


class TestProject(TestCase):
    def test(self):
        constraint = [
            [{'attribute': 'A1', 'operator': '>', 'value': 4}],
            [{'attribute': 'A1', 'operator': '=', 'value': 3},
             {'attribute': 'A2', 'operator': '<', 'value': 179}],
            [{'attribute': 'A2', 'operator': '<>', 'value': 16},
             {'attribute': 'A3', 'operator': '<=', 'value': 13}]
        ]
        attributes = ['A2', 'A3']

        projection = project(constraint, attributes)

        self.assertEqual(len(projection), 3)
        self.assertEqual(projection[0], [])  # first sublist should be gone
        self.assertEqual(projection[1], [constraint[1][1]])
        self.assertEqual(projection[2], constraint[2])
