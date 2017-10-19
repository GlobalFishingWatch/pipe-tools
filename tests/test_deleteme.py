import pytest
from datetime import datetime
from datetime import timedelta
import json
import udatetime

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.utils.timestamp import Timestamp
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import coders

from pipe_tools.timestamp import *

@pytest.mark.skip(reason="tests that should be deleted")
@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestDeleteMe():
    """
    These tests should be deleted.  This is just a scrath workspace
    """

    # def test_date_partitioned_file_write(self):
    #     def _sample_data(self):
    #         start_ts = datetime2timestamp(datetime(2017, 1, 1, 0, 0, 0))
    #         increment = 100.0
    #         count = 864 * 2
    #         ts = start_ts
    #         for t in xrange(count):
    #             yield {'mmsi': 1, 'timestamp': ts}
    #             ts += increment
    #
    #     with _TestPipeline() as p:
    #         messages = (
    #             p
    #             | beam.Create(self._sample_data())
    #         )
    #

    def test_bigquery(self):

        pipeline_options = PipelineOptions(project='world-fishing-827',
                                           runner='DirectRunner')

        source = beam.io.gcp.bigquery.BigQuerySource(
            table='world-fishing-827:scratch_paul.shard_test_a_20170101')

        day = timedelta(days=1).total_seconds()

        with _TestPipeline(options=pipeline_options) as p:
            result = (
                p | beam.io.Read(source)
                | beam.ParDo(ParseBeamBQStrTimestampDoFn())
                | beam.WindowInto(window.FixedWindows(day))
                | beam.ParDo(AddWindow())
                # | beam.io.WriteToBigQuery(table='world-fishing-827:scratch_paul.timestamp_test_b',
                #                           schema='mmsi:INTEGER,old_ts:TIMESTAMP,new_ts:TIMESTAMP')
            )

    def test_window_params(self):
        # TODO: just exploring here.  Should probably delete this

        with _TestPipeline() as p:
            result = (
                p | beam.Create([k for k in range(0,10)])
                | beam.Map(lambda k: (TimestampedValue(k, k)))
                #
                # # wrap all this in our custom writer
                # | core.WindowInto(window.GlobalWindows())
                # | beam.Map(lambda k: ((partition, shard), k))) # Assign days and shards
                # | beam.GroupByKey() # group by day by shard
                # | write to file, one group per shard
                #     # in writer.write, output to separate temp for each group and store (partition, tempfile)
                #     # attach the partition to the return(s) from close

                | beam.ParDo(DoNothing())
            )
            assert_that(result, equal_to([(None, [0, 1, 2, 3, 4]), (None, [5, 6, 7, 8, 9])]))


class AddWindow(beam.DoFn):
    def __init__(self):
        self._window_start=None

    def start_bundle(self):
        pass


    def process(self, element, window=beam.DoFn.WindowParam):
        yield element


class TransformTimestamp(beam.DoFn):

    def process(self, element, window=beam.DoFn.WindowParam):
        old_ts = element['timestamp']
        new_ts = str2timestamp(old_ts)

        yield dict(mmsi=element['mmsi'],
                   old_ts=old_ts,
                   new_ts=new_ts)

class DoNothing(beam.DoFn):

    def start_bundle(self):
        pass

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        # k,v = element
        print timestamp
        yield element
