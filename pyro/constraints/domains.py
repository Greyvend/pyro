import math


class Domain:
    def issubset(self, other):
        return NotImplementedError()

    def _contains_elements(self, elements):
        return NotImplementedError()

    def _contains_interval(self, lower, upper, lower_deleted=False,
                           upper_deleted=False):
        return NotImplementedError()


# noinspection PySetFunctionToLiteral,PyProtectedMember
class Point(Domain):
    def __init__(self, value):
        self.value = value

    def issubset(self, other):
        return other._contains_elements([self.value])

    def _contains_elements(self, elements):
        return set(elements) == set([self.value])

    def _contains_interval(self, lower_point, upper_point, lower_deleted=False,
                           upper_deleted=False):
        return False

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        if self.value == other.value:
            return False
        if self.value == -math.inf or other.value == math.inf:
            return True
        if self.value == math.inf or other.value == -math.inf:
            return False
        return self.value < other.value

    def __le__(self, other):
        return self < other or self == other


class Interval(Domain):
    """
    Class representing a mathematical interval for ordered types, such as
    ints, floats, dates & times.

    Intervals can be open and closed (sectors). The inclusion of the borders
    is controlled by `lower_deleted` and `upper_deleted` parameters on
    construction.
    """
    def __init__(self, lower=None, upper=None, lower_deleted=False,
                 upper_deleted=False):
        self.lower = Point(lower) if lower is not None else Point(-math.inf)
        self.lower_deleted = lower_deleted
        self.upper = Point(upper) if upper is not None else Point(math.inf)
        self.upper_deleted = upper_deleted

    def issubset(self, other):
        return other._contains_interval(self.lower, self.upper,
                                        self.lower_deleted, self.upper_deleted)

    def _contains_elements(self, elements):
        return self._contains_interval(Point(min(elements)),
                                       Point(max(elements)))

    def _contains_interval(self, lower_point, upper_point, lower_deleted=False,
                           upper_deleted=False):
        if self.lower > lower_point:
            return False
        if self.lower == lower_point and self.lower_deleted and \
                not lower_deleted:
            return False
        if self.upper < upper_point:
            return False
        if self.upper == upper_point and self.upper_deleted and \
                not upper_deleted:
            return False
        return True


class Set(Domain):
    """
    Class representing a finite set of values for any supported data type.
    """
    def __init__(self, elements):
        self.elements = set(elements)

    def issubset(self, other):
        return other._contains_elements(self.elements)

    def _contains_elements(self, elements):
        return self.elements.issuperset(set(elements))

    def _contains_interval(self, lower_point, upper_point, lower_deleted=False,
                           upper_deleted=False):
        return False


class PunctuatedDomain(Domain):
    def __init__(self, deleted_point):
        self.deleted_point = deleted_point
        self.lower_interval = Interval(-math.inf, deleted_point,
                                       upper_deleted=True)
        self.upper_interval = Interval(deleted_point, math.inf,
                                       lower_deleted=True)

    def issubset(self, other):
        lower_issubset = self.lower_interval.issubset(other)
        upper_issubset = self.upper_interval.issubset(other)
        return lower_issubset and upper_issubset

    def _contains_elements(self, elements):
        return self.deleted_point not in elements

    def _contains_interval(self, lower, upper, lower_deleted=False,
                           upper_deleted=False):
        lower_contains = self.lower_interval._contains_interval(lower, upper,
                                                                lower_deleted,
                                                                upper_deleted)
        upper_contains = self.upper_interval._contains_interval(lower, upper,
                                                                lower_deleted,
                                                                upper_deleted)
        return lower_contains or upper_contains


class Pattern(Domain):
    """
    Class representing a regular expression. Acts as SQL LIKE operation
    abstraction.

    At the moment only direct comparisons are available for regular
    expressions.
    """
    def __init__(self, regexp):
        self.regexp = regexp

    def issubset(self, other):
        return other._contains_elements([self.regexp])

    # noinspection PySetFunctionToLiteral
    def _contains_elements(self, elements):
        # TODO: it can be achieved by using algorithms described here:
        # http://stackoverflow.com/questions/18729015/determining-whether-a-regex-is-a-subset-of-another
        return set([self.regexp]) == set(elements)
