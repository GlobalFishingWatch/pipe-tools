import ujson
import apache_beam as beam
from apache_beam import typehints
from apache_beam import PTransform


class JSONDictCoder(beam.coders.Coder):
    """A coder used for reading and writing json"""

    def encode(self, value):
        return ujson.dumps(value)

    def decode(self, value):
        return ujson.loads(value)

    def is_deterministic(self):
        return True

JSONDict = typehints.Dict[str, typehints.Any]

