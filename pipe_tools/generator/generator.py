from datetime import datetime
import pytz
from pipe_tools.timestamp import timestampFromDatetime


DEFAULT_START_TS = timestampFromDatetime(datetime(2017, 1, 1, 0, 0, 0, tzinfo=pytz.UTC))
HOUR_IN_SECONDS = 60 * 60


class TimestampMessageGenerator():
    def __init__(self, start_ts=DEFAULT_START_TS,
                 increment=HOUR_IN_SECONDS, count=72):
        self.start_ts = start_ts
        self.increment = increment
        self.count = count

    def __iter__(self):
        return self.messages()

    def messages(self):
        ts = self.start_ts
        for idx in xrange(self.count):
            yield {'mmsi': 1, 'timestamp': ts, 'idx': idx}
            ts += self.increment

    def bigquery_schema(self):
        return "mmsi:INTEGER,timestamo:TIMESTAMP,idx:INTEGER"
