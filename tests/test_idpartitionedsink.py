import posixpath as pp

import pytest
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue

from pipe_tools.io.idpartitionedsink import IdPartitionedFileSink
from pipe_tools.io.idpartitionedsink import WriteToIdPartitionedFiles
from pipe_tools.timestamp import *


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestDatePartitionedSink():
    """
    Tests for DatePartitionedSink and related classes
    """

    def _sample_data(self, stringify):
        count = 100
        for vessel_id in xrange(count):
            if stringify:
                vessel_id = str(vessel_id)
            yield JSONDict(id=vessel_id)

    def test_as_pipeline_numbers(self, temp_dir):

        file_path_base = temp_dir
        file_name_prefix = 'shard'
        file_path_prefix = pp.join(file_path_base, file_name_prefix)
        file_name_suffix = '.json'

        messages = list(self._sample_data(stringify=False))

        with _TestPipeline() as p:
            messages = (
                p
                | beam.Create(messages)
            )

            result = messages | WriteToIdPartitionedFiles(file_path_prefix, file_name_suffix, shards_per_id=1)
            expected = ['%s%s%s'% (pp.join(file_path_base, str(i), file_name_prefix),
                                   '-00000-of-00001', file_name_suffix) for i in range(100)]
            assert_that(result, equal_to(expected))

    def test_as_pipeline_strings(self, temp_dir):

        file_path_base = temp_dir
        file_name_prefix = 'shard'
        file_path_prefix = pp.join(file_path_base, file_name_prefix)
        file_name_suffix = '.json'

        messages = list(self._sample_data(stringify=True))

        with _TestPipeline() as p:
            messages = (
                p
                | beam.Create(messages)
            )

            result = messages | WriteToIdPartitionedFiles(file_path_prefix, file_name_suffix, shards_per_id=1)
            expected = ['%s%s%s'% (pp.join(file_path_base, str(i), file_name_prefix),
                                   '-00000-of-00001', file_name_suffix) for i in range(100)]
            assert_that(result, equal_to(expected))