from datetime import datetime
from unittest import TestCase
import math

from pyro.constraints.domains import Point


class TestPoint(TestCase):
    def test_issubset_other_point_number(self):
        point_1 = Point(33.5)
        point_2 = Point(66.5)
        point_4 = Point(33.50)

        self.assertTrue(point_1.issubset(point_1))
        self.assertFalse(point_1.issubset(point_2))
        self.assertFalse(point_2.issubset(point_1))
        self.assertTrue(point_1.issubset(point_4))
        self.assertTrue(point_4.issubset(point_1))

    def test_issubset_other_point_datetime(self):
        dt = datetime(2016, 12, 16, 21, 18)

        point_1 = Point(dt)
        point_2 = Point(dt)
        point_3 = Point(datetime(2016, 9, 16, 21, 18))

        self.assertTrue(point_1.issubset(point_1))
        self.assertTrue(point_1.issubset(point_2))
        self.assertTrue(point_2.issubset(point_1))
        self.assertFalse(point_1.issubset(point_3))

    def test_issubset_other_point_string(self):
        s = 'random string'

        point_1 = Point(s)
        point_2 = Point(s)
        point_3 = Point('another string')

        self.assertTrue(point_1.issubset(point_1))
        self.assertTrue(point_1.issubset(point_2))
        self.assertTrue(point_2.issubset(point_1))
        self.assertFalse(point_1.issubset(point_3))

    def test_contains_interval_simple_included(self):
        point = Point(33.5)
        lower = Point(30.0)
        upper = Point(66.5)

        self.assertFalse(point._contains_interval(lower, upper))

    def test_contains_interval_simple_border(self):
        point = Point(33.5)
        lower = Point(33.5)
        upper = Point(66.5)

        self.assertFalse(point._contains_interval(lower, upper))

    def test_contains_interval_point(self):
        point = Point(33.5)
        lower = Point(33.5)
        upper = Point(33.5)

        self.assertTrue(point._contains_interval(lower, upper))

    def test_eq_numbers(self):
        point_1 = Point(1)
        point_2 = Point(1.0)
        point_3 = Point(1.5)
        inf = Point(math.inf)
        minus_inf = Point(-math.inf)

        self.assertTrue(point_1 == point_1)
        self.assertTrue(point_1 == point_2)
        self.assertFalse(point_1 != point_2)
        self.assertFalse(point_1 == point_3)
        self.assertFalse(point_1 == inf)
        self.assertFalse(point_1 == minus_inf)
        self.assertFalse(minus_inf == inf)

    def test_lt_numbers(self):
        point_1 = Point(1)
        point_2 = Point(-1.5)
        point_3 = Point(1.33333)
        inf = Point(math.inf)
        minus_inf = Point(-math.inf)

        self.assertFalse(point_1 < point_1)
        self.assertFalse(point_1 < point_2)
        self.assertTrue(point_1 < point_3)
        self.assertTrue(point_1 < inf)
        self.assertTrue(point_2 < inf)
        self.assertTrue(point_3 < inf)
        self.assertFalse(point_1 < minus_inf)
        self.assertTrue(minus_inf < point_1)
        self.assertTrue(minus_inf < point_2)
        self.assertTrue(minus_inf < point_3)
        self.assertTrue(minus_inf < inf)
        self.assertFalse(inf < minus_inf)
        self.assertFalse(inf < inf)
        self.assertFalse(minus_inf < minus_inf)

    def test_lt_dates(self):
        point_1 = Point(datetime(2016, 9, 16, 21, 18))
        point_2 = Point(datetime(2016, 12, 16, 21, 18))
        inf = Point(math.inf)
        minus_inf = Point(-math.inf)

        self.assertFalse(point_1 < point_1)
        self.assertTrue(point_1 < point_2)
        self.assertTrue(point_1 < inf)
        self.assertTrue(point_2 < inf)
        self.assertFalse(point_1 < minus_inf)
        self.assertTrue(minus_inf < point_1)
        self.assertTrue(minus_inf < point_2)
        self.assertTrue(minus_inf < inf)
        self.assertFalse(inf < minus_inf)

    def test_le_numbers(self):
        point_1 = Point(1)
        point_2 = Point(1.0)
        point_3 = Point(1.5)
        inf = Point(math.inf)
        minus_inf = Point(-math.inf)

        self.assertTrue(point_1 <= point_1)
        self.assertTrue(point_1 <= point_2)
        self.assertTrue(point_2 <= point_1)
        self.assertTrue(point_1 <= point_3)
        self.assertFalse(point_3 <= point_1)
        self.assertTrue(point_1 <= inf)
        self.assertTrue(point_2 <= inf)
        self.assertTrue(point_3 <= inf)
        self.assertFalse(point_1 <= minus_inf)
        self.assertTrue(minus_inf <= point_1)
        self.assertTrue(minus_inf <= point_2)
        self.assertTrue(minus_inf <= point_3)
        self.assertTrue(minus_inf <= inf)
        self.assertFalse(inf <= minus_inf)