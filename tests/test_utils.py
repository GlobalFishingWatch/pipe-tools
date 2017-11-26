import pytest

from pipe_tools.utils import flatten


class Test_Utils:
    @pytest.mark.parametrize("value,expected", [
        (None,[]),
        (42, [42]),
        ('foo', ['foo']),
        (['foo', ['bar', 'troll']], ['foo', 'bar', 'troll']),
        ({'a': 'foo', 'b': 'bar'}, ['foo', 'bar']),
    ])
    def test_flatten(self, value, expected):
        assert flatten(value) == expected
