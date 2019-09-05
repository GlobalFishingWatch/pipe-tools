import posixpath as pp

import pytest
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.io import FileBasedSink

from pipe_tools.io.datepartitionedsink import DatePartitionedFileSink
from pipe_tools.io.datepartitionedsink import WriteToDatePartitionedFiles
from pipe_tools.timestamp import *


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestDatePartitionedSink():
    """
    Tests for DatePartitionedSink and related classes
    """

    def _sample_data(self):
        start_ts = timestampFromDatetime(datetime(2017, 1, 1, 0, 0, 0, tzinfo=pytz.UTC))
        increment = 60 * 60  # 1 hour
        count = 24 * 3  # 3 days
        ts = start_ts
        for t in xrange(count):
            yield dict(mmsi=1, timestamp= ts)
            ts += increment

    @pytest.mark.parametrize("shards_per_day", [1,2,3])
    def test_as_pipeline(self, temp_dir, shards_per_day):

        file_path_base = temp_dir
        # file_path_base = 'gs://paul-scratch/TestDatePartitionedSink_temp'
        file_name_prefix = 'shard'
        file_path_prefix = pp.join(file_path_base, file_name_prefix)
        file_name_suffix = '.json'

        messages = list(self._sample_data())
        dates = {datetimeFromTimestamp(msg['timestamp']).strftime(DatePartitionedFileSink.DATE_FORMAT)
                 for msg in messages}

        with _TestPipeline() as p:
            writer = WriteToDatePartitionedFiles(file_path_prefix, file_name_suffix,
                                                            shards_per_day=shards_per_day)
            messages = (
                p
                | beam.Create(messages)
                | beam.Map(lambda msg: (TimestampedValue(msg, msg['timestamp'])))
            )

            result = messages | writer

            expected = []
            for date in dates:
                for shard_num in range(shards_per_day):
                    expected.append (
                        ''.join([pp.join(file_path_base, date, file_name_prefix), writer._sink.shard_name_format % dict(
                            shard_num=shard_num, num_shards=shards_per_day), file_name_suffix]
                    ))

            assert_that(result, equal_to(expected))

