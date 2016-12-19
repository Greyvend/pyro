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
        same_value = lower_point.value == upper_point.value == self.value
        not_deleted = not (lower_deleted or upper_deleted)
        return same_value and not_deleted

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
