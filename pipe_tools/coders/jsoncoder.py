import ujson
import apache_beam as beam
import six
from apache_beam import typehints
from apache_beam import PTransform


class JSONDictCoder(beam.coders.Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return bytes(ujson.dumps(value), 'UTF-8')

    def decode(self, value):
        return ujson.loads(six.text_type(value, 'UTF-8'))

    def is_deterministic(self):
        return True

JSONDict = typehints.Dict[six.binary_type, typehints.Any]

