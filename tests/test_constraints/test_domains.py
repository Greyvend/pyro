from datetime import datetime
from unittest import TestCase
import math

from pyro.constraints.domains import Point, Interval


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


class TestInterval(TestCase):
    def test_construction(self):
        interval_1 = Interval(lower=0, upper=10, lower_deleted=True)

        self.assertEqual(interval_1.lower, Point(0))
        self.assertTrue(interval_1.lower_deleted)
        self.assertEqual(interval_1.upper, Point(10))
        self.assertFalse(interval_1.upper_deleted)

        interval_2 = Interval(lower=0)

        self.assertEqual(interval_2.lower, Point(0))
        self.assertEqual(interval_2.upper, Point(math.inf))

        interval_3 = Interval(upper=10)

        self.assertEqual(interval_3.upper, Point(10))
        self.assertEqual(interval_3.lower, Point(-math.inf))

    def test_issubset_sectors_numbers(self):
        interval_1 = Interval(lower=0, upper=10)
        interval_2 = Interval(lower=0, upper=10)
        interval_3 = Interval(lower=0, upper=9)
        interval_4 = Interval(lower=1, upper=10)
        interval_5 = Interval(lower=0)
        interval_6 = Interval(lower=1)
        interval_7 = Interval(upper=10)
        interval_8 = Interval(upper=9.5)

        self.assertTrue(interval_1.issubset(interval_1))
        self.assertTrue(interval_1.issubset(interval_2))
        self.assertFalse(interval_1.issubset(interval_3))
        self.assertTrue(interval_3.issubset(interval_1))
        self.assertFalse(interval_1.issubset(interval_4))
        self.assertTrue(interval_4.issubset(interval_1))
        self.assertTrue(interval_1.issubset(interval_5))
        self.assertFalse(interval_5.issubset(interval_1))
        self.assertFalse(interval_1.issubset(interval_6))
        self.assertFalse(interval_6.issubset(interval_1))
        self.assertFalse(interval_5.issubset(interval_6))
        self.assertTrue(interval_6.issubset(interval_5))
        self.assertTrue(interval_1.issubset(interval_7))
        self.assertFalse(interval_7.issubset(interval_1))
        self.assertFalse(interval_1.issubset(interval_8))
        self.assertFalse(interval_8.issubset(interval_1))

    def test_issubset_open_intervals_numbers(self):
        interval_1 = Interval(lower=0, upper=10,  lower_deleted=True)
        interval_2 = Interval(lower=0, upper=10)
        interval_3 = Interval(lower=0, upper=10, upper_deleted=True)
        interval_4 = Interval(lower=0, upper=10, lower_deleted=True,
                              upper_deleted=True)

        self.assertTrue(interval_1.issubset(interval_2))
        self.assertFalse(interval_2.issubset(interval_1))
        self.assertFalse(interval_2.issubset(interval_3))
        self.assertTrue(interval_3.issubset(interval_2))
        # test fully open interval
        self.assertTrue(interval_4.issubset(interval_1))
        self.assertTrue(interval_4.issubset(interval_2))
        self.assertTrue(interval_4.issubset(interval_3))
        self.assertFalse(interval_1.issubset(interval_4))
        self.assertFalse(interval_2.issubset(interval_4))
        self.assertFalse(interval_3.issubset(interval_4))

    def test_issubset_datetimes(self):
        interval_1 = Interval(lower=datetime(2016, 9, 16, 21, 18),
                              upper=datetime(2016, 10, 19, 21, 18))
        interval_2 = Interval(lower=datetime(2016, 9, 16, 21, 18))

        self.assertTrue(interval_1.issubset(interval_2))
        self.assertFalse(interval_2.issubset(interval_1))

    def test_contains_elements_numbers(self):
        interval = Interval(lower=0, upper=10)

        self.assertTrue(interval._contains_elements([0, 2, 10.0, 3, 7.5]))
        self.assertFalse(interval._contains_elements([-7, 2, 10, 3, 7.5]))
        self.assertFalse(interval._contains_elements([0, 2, 19]))

    def test_contains_elements_dates(self):
        interval = Interval(lower=datetime(2016, 3, 15),
                            upper=datetime(2016, 9, 22))

        self.assertTrue(interval._contains_elements([datetime(2016, 3, 15),
                                                     datetime(2016, 4, 19),
                                                     datetime(2016, 7, 1)]))
        self.assertFalse(interval._contains_elements([datetime(2016, 3, 14),
                                                      datetime(2016, 4, 19),
                                                      datetime(2016, 7, 1)]))
        self.assertFalse(interval._contains_elements([datetime(2016, 3, 15),
                                                      datetime(2016, 10, 19),
                                                      datetime(2016, 7, 1)]))
