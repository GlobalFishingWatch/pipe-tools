import pytest
import pytz
import udatetime

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from datetime import datetime
from pipe_tools.timestamp import *


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestTimestampTools():

    def test_timestamp_conversions(self):

        now = datetime.now(tz=pytz.UTC)

        assert datetimeFromTimestamp(timestampFromDatetime(now)) == now
        assert datetimeFromBeamBQStr(beambqstrFromDatetime(now)) == now

        u_now = udatetime.utcnow()
        assert udatetimeFromTimestamp(timestampFromUdatetime(u_now)) == u_now

        ts_now = timestampFromUdatetime(u_now)
        assert timestampFromBeamBQStr(beambqstrFromTimestamp(ts_now)) == ts_now
        assert timestampFromRfc3339str(rfc3339strFromTimestamp(ts_now)) == ts_now


    def test_ParseBeamBQStrTimestamp(self):
        r = list(range(10))
        source = [{'timestamp':beambqstrFromTimestamp(t)} for t in r] + [{'timestamp':None}]
        expected = [{'timestamp':t} for t in r]+ [{'timestamp':None}]
        timestamp_fields = 'timestamp'

        with _TestPipeline() as p:
            fields = p | beam.Create(source).with_output_types(JSONDict)
            safe = fields | beam.ParDo(SafeParseBeamBQStrTimestampDoFn(fields=timestamp_fields))

            assert_that(safe, equal_to(expected), label='safe')

        with _TestPipeline() as p:
            fields = p | beam.Create(source).with_output_types(JSONDict)
            fast = fields | beam.ParDo(ParseBeamBQStrTimestampDoFn(fields=timestamp_fields))

            assert_that(fast, equal_to(expected), label='fast')
