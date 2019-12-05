import pytest
import posixpath as pp
import newlinejson as nlj
import ujson
import six

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards

from pipe_tools.io.gcp import parse_gcp_path
from pipe_tools.io.gcp import GCPSource
from pipe_tools.io.gcp import GCPSink
from pipe_tools.generator import GenerateMessages
from pipe_tools.generator import MessageGenerator

def fix_keys(d):
    """We want our dicts to always be keyed by bytes

    This is for beam compatibility, but json doesn't roundtrip bytes.
    """
    return {six.ensure_binary(k) : v for (k, v) in d.items()}



@pytest.mark.filterwarnings('ignore::FutureWarning')
class Test_IO_GCP:
    @pytest.mark.parametrize("value,expected", [
        ('bq://project:dataset.table', ('table', 'project:dataset.table')),
        ('query://select * from [Table]', ('query', 'select * from [Table]')),
        ('gs://bucket/path/file', ('file', 'gs://bucket/path/file')),
        ('file:///path/file', ('file', '/path/file')),
        ('/path/file', ('file', '/path/file')),
        ('./path/file', ('file', './path/file')),
        ('select * from [Table]', ('query', 'select * from [Table]'))
    ])
    def test_parse_gcp_path(self, value, expected):
        assert parse_gcp_path(value) == expected

    def test_gcp_sink(self, temp_dir):
        messages = list(MessageGenerator().messages())
        dest = pp.join(temp_dir, 'messages.json')

        with _TestPipeline() as p:
            ( p | beam.Create(messages) | GCPSink(dest) )
        p.run()

        with open_shards('%s*' % dest) as output:
            assert (sorted(messages, key=lambda x:x[b'timestamp']) == 
                    sorted(
                        [fix_keys(d) for d in nlj.load(output)], 
                          key=lambda x:x[b'timestamp']))


    def test_gcp_source(self, temp_dir):
        expected = list(MessageGenerator().messages())
        source = pp.join(temp_dir, 'messages.json')
        with open(source, 'w') as f:
            nlj.dump(expected, f, json_lib=ujson)

        with _TestPipeline() as p:
            messages = p | GCPSource(source)
        p.run()

        assert_that (messages, equal_to(expected))
