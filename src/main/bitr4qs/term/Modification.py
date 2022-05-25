from .Quad import Quad
from .Triple import Triple


class Modification(object):

    def __init__(self, value, deletion=False):
        assert isinstance(value, Quad) or isinstance(value, Triple), \
            "Modification value %s must be a Triple or Quad." % (value,)
        self._value = value
        self._deletion = deletion

    @property
    def deletion(self):
        return self._deletion

    @property
    def insertion(self):
        return not self._deletion

    @property
    def value(self):
        return self._value

    def invert(self):
        if self._deletion:
            self._deletion = False
        else:
            self._deletion = True

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self._value != other.value:
                return False
            if self._deletion != other.deletion:
                return False
            return True
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)
