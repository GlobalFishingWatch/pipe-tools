import pytest

from pipe_tools.utils import flatten
from pipe_tools.utils import GFW_UUID


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

    def test_gfw_uuid(self):
        assert str(GFW_UUID('test')) == 'c3757317-71ed-5251-bec6-fa01f05cb8dc'
