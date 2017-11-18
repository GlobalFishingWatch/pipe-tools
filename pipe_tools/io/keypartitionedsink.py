import random
import warnings

from .partitionedsink import WritePartitionedFiles
from .partitionedsink import PartitionedFileSink
from .partitionedsink import T

import apache_beam as beam
from apache_beam import PTransform
from apache_beam import core
from apache_beam.transforms import window
from apache_beam.io.filesystem import CompressionTypes

from pipe_tools.coders import JSONDictCoder
from pipe_tools.coders import JSONDict

from apache_beam import typehints
from apache_beam.typehints import Tuple, KV


DEFAULT_SHARDS_PER_KEY = 1


class WriteToKeyPartitionedFiles(WritePartitionedFiles):
    """
    Write the incoming pcoll to files partitioned by a string key.  The key is taken from the
    a field in each element specified by 'key'.

    """
    def __init__(self,
                 key_field,
                 file_path_prefix,
                 file_name_suffix='',
                 append_trailing_newlines=True,
                 shards_per_key=DEFAULT_SHARDS_PER_KEY,
                 shard_name_template=None,
                 coder=JSONDictCoder(),
                 compression_type=CompressionTypes.AUTO,
                 header=None):

        self.shards_per_key = shards_per_key or DEFAULT_SHARDS_PER_KEY

        self._sink = KeyPartitionedFileSink(file_path_prefix,
                                             file_name_suffix=file_name_suffix,
                                             append_trailing_newlines=append_trailing_newlines,
                                             shard_name_template=shard_name_template,
                                             coder=coder,
                                             compression_type=compression_type,
                                             header=header)

        self._sharder = KeyShardDoFn(key_field, shards_per_key=self.shards_per_key)





@typehints.with_input_types(T)
@typehints.with_output_types(KV[Tuple[str,int],T])
class KeyShardDoFn(beam.DoFn):
    """
    Apply id and shard number
    """

    def __init__(self, key, shards_per_key=None):
        self.key = key
        self.shards_per_key = shards_per_key or DEFAULT_SHARDS_PER_KEY
        self.shard_counter = 0

    def start_bundle(self):
        self.shard_counter = random.randint(0, self.shards_per_key - 1)

    def process(self, element):
        element_key = str(element[self.key])
        shard = self.shard_counter

        self.shard_counter += 1
        if self.shard_counter >= self.shards_per_key:
            self.shard_counter -= self.shards_per_key
        assert isinstance(element, JSONDict), 'element must be a JSONDict'
        yield ((element_key, shard), element)


class KeyPartitionedFileSink(PartitionedFileSink):
    def _encode_key(self, key):
        """convert an id to a string representation"""
        return key

