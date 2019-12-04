import posixpath as pp

import pytest
import six
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.io.keypartitionedsink import WriteToKeyPartitionedFiles
from pipe_tools.timestamp import *


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestKeyPartitionedSink():
    """
    Tests for KeyPartitionedSink and related classes
    """

    def _sample_data(self, stringify, key='id', count=100):
        for vessel_id in six.moves.range(count):
            if stringify:
                vessel_id = str(vessel_id)
            if isinstance(key, six.text_type):
                key = bytes(key, 'UTF-8')
            yield {key: vessel_id}

    @pytest.mark.parametrize("stringify,key,count,shard_name_template", [
        (False, b'id', 100, None),
        (True, b'id', 100, None),
        (True, b'boa', 100, None),
        (True, b'id', 100, ''),
        (True, b'id', 0, ''),
    ])
    def test_expected_shard_files(self, temp_dir, stringify, key, count, shard_name_template):
        file_path_base = temp_dir
        file_name_prefix = 'shard'
        file_path_prefix = pp.join(file_path_base, file_name_prefix)
        file_name_suffix = '.json'

        messages = list(self._sample_data(stringify=stringify, key=key, count=count))

        with _TestPipeline() as p:
            messages = p | beam.Create(messages)

            result = messages | WriteToKeyPartitionedFiles(key, file_path_prefix, file_name_suffix,
                                                           shard_name_template=shard_name_template)

            if shard_name_template is None:
                expected = ["%s%s%s"% (pp.join(file_path_base, str(i), file_name_prefix),
                                   '-00000-of-00001', file_name_suffix) for i in range(count)]
            else:
                # Of the form ..../shardN.json
                expected = ['%s%s%s' % (pp.join(file_path_base, file_name_prefix), str(i),
                                        file_name_suffix) for i in range(count)]

            assert_that(result, equal_to(expected))
