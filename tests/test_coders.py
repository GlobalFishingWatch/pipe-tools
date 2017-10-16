import pytest
import ujson

from pipe_tools.coders import JSONCoder


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestCoders():

    def test_JSONCoder(self):
        records = [
            0,
            {'a': 1, 'b': 2, 'c': None},
            "test",
            None
        ]
        coder = JSONCoder()
        for r in records:
            assert r == coder.decode(coder.encode(r))
            assert ujson.dumps(r) == coder.encode(r)
