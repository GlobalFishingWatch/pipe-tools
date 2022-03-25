import ujson
import apache_beam as beam
import six
import datetime
import pytz
import copy
from apache_beam import typehints
from apache_beam import PTransform


epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)

def _datetime_to_s(x):
    return (x - epoch).total_seconds()

def _s_to_datetime(x):
    return epoch + datetime.timedelta(seconds=x)

class JSONDictCoder(beam.coders.Coder):
    """A coder used for reading and writing json"""

    def __init__(self, time_fields = []):
        self.time_fields = time_fields

    def _encode(self, value):
        replacements = {x: _datetime_to_s(value.get(x)) for x in self.time_fields}
        new_value = copy.deepcopy(value)
        new_value.update(replacements)
        print(replacements, value, new_value)
        return new_value

    def encode(self, value):
        return six.ensure_binary(ujson.dumps(self._encode(value)))

    def _decode(self, value):
        replacements = {x: _s_to_datetime(value.get(x)) for x in self.time_fields}
        new_value = copy.deepcopy(value)
        new_value.update(replacements)
        return new_value

    def decode(self, value):
        return self._decode(ujson.loads(six.ensure_str(value)))

    def is_deterministic(self):
        return True

JSONDict = typehints.Dict[six.binary_type, typehints.Any]

