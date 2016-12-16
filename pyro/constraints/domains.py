class Domain:
    def issubset(self, other):
        return NotImplementedError()

    def _contains_elements(self, elements):
        return NotImplementedError()

    def _contains_interval(self, lower_boundary, upper_boundary):
        return NotImplementedError()


# noinspection PySetFunctionToLiteral,PyProtectedMember
class Point(Domain):
    def __init__(self, value, deleted=False):
        self.value = value
        self.deleted = deleted

    def issubset(self, other):
        assert not self.deleted
        return other._contains_elements([self.value])

    def _contains_elements(self, elements):
        assert not self.deleted
        return set(elements) == set([self.value])

    def _contains_interval(self, lower_boundary, upper_boundary):
        assert not self.deleted
        same_value = lower_boundary.value == upper_boundary.value == self.value
        not_deleted = not (lower_boundary.deleted or upper_boundary.deleted)
        return same_value and not_deleted
