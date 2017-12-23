from datetime import datetime
import pytz
from dateutil.parser import parse as dateutil_parse

from apache_beam.utils.timestamp import Timestamp

from pipe_tools.timestamp import timestampFromDatetime

# TODO:  Move all the other timestamp helpers here


def as_timestamp(d):
    """
    Attempt to convert the passed parameter to a datetime.  Note that for strings this
    uses python-dateutil, which is flexible, but SLOW.  So do not use this methig if you
    are going to be parsing a billion dates.  Use the high performance methods in timestamp.py

    return: float   seconds since epoch (unix timestamp)
    """
    if d is None:
        return None
    elif isinstance(d,datetime):
        return timestampFromDatetime(pytz.UTC.localize(d) if d.tzinfo is None else d)
        # return timestampFromDatetime(pytz.UTC.localize(d))
    elif isinstance(d, (float, int, Timestamp)):
        return float(d)
    elif isinstance(d, basestring):
        return as_timestamp(dateutil_parse(d))
    else:
        raise ValueError ('Unsupported data type. Unable to convert value "%s" to to a timestamp' % d)