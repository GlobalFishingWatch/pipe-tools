import pytest
from datetime import datetime
from datetime import date

from pipe_tools.utils import flatten
from pipe_tools.utils import GFW_UUID
from pipe_tools.utils.timestamp import as_timestamp

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

    @pytest.mark.parametrize("value,expected", [
        (None,None),
        ("2017-07-20T05:59:35.000000Z", 1500530375.0),
        (datetime(2017,7,20,5,59,35), 1500530375.0),
        (1500530375.0, 1500530375.0),
        (date(2018,1,1), 1514764800.0)
    ])
    def test_as_timestamp(self, value, expected):
        assert as_timestamp(value) == expected

    def test_as_timestamp_raises(self):
        with pytest.raises(ValueError):
            as_timestamp([1,2,3])