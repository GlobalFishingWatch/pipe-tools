import pytest
import ujson

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.coders import typecoders

from pipe_tools.coders import JSONDictCoder
from pipe_tools.coders import JSONDict
from pipe_tools.generator import MessageGenerator




class MyType():
    pass


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestCoders():

    def test_JSONDictCoder(self):
        records = [
            {},
            {'a': 1, 'b': 2, 'c': None},
            {"test":None},
        ]
        coder = JSONDictCoder()
        for r in records:
            assert r == coder.decode(coder.encode(r))
            assert ujson.dumps(r) == coder.encode(r)

    def test_type_hints(self):

        messages = MessageGenerator()

        source = beam.Create(messages)
        assert source.get_output_type() == Dict[str, Union[float, int]], 
                                                (source.get_output_type(), JSONDict)

        with _TestPipeline() as p:
            result = (
                p | beam.Create(messages)
            )

            p.run()