import random
import warnings

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


DEFAULT_SHARDS_PER_ID = 1


@typehints.with_input_types(JSONDict)
@typehints.with_output_types(str)
class WriteToIdPartitionedFiles(PTransform):
    """
    Write the incoming pcoll to files partitioned by Id.  The id is taken from the
    Id field of each element.

    """
    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='',
                 append_trailing_newlines=True,
                 shards_per_id=DEFAULT_SHARDS_PER_ID,
                 shard_name_template=None,
                 coder=JSONDictCoder(),
                 compression_type=CompressionTypes.AUTO,
                 header=None):

        self.shards_per_id = shards_per_id or DEFAULT_SHARDS_PER_ID

        self._sink = IdPartitionedFileSink(file_path_prefix,
                                             file_name_suffix=file_name_suffix,
                                             append_trailing_newlines=append_trailing_newlines,
                                             shard_name_template=shard_name_template,
                                             coder=coder,
                                             compression_type=compression_type,
                                             header=header)

    def expand(self, pcoll):
        pcoll = (
            pcoll
            | core.WindowInto(window.GlobalWindows())
            | beam.ParDo(IdShardDoFn(shards_per_id=self.shards_per_id))
            | beam.GroupByKey()   # group by id and shard
        )
        with warnings.catch_warnings():
            # suppress a spurious warning generated within beam.io.Write.  This warning is annoying but harmless
            warnings.filterwarnings(action="ignore", message="Using fallback coder for typehint: <type 'NoneType'>")

            return pcoll | beam.io.Write(self._sink).with_output_types(str)




@typehints.with_input_types(T)
@typehints.with_output_types(KV[Tuple[str,int],T])
class IdShardDoFn(beam.DoFn):
    """
    Apply id and shard number
    """

    def __init__(self, shards_per_id=None):
        self.shards_per_id = shards_per_id or DEFAULT_SHARDS_PER_ID
        self.shard_counter = 0

    def start_bundle(self):
        self.shard_counter = random.randint(0, self.shards_per_id - 1)

    def process(self, element):
        element_id = str(element['id'])
        shard = self.shard_counter

        self.shard_counter += 1
        if self.shard_counter >= self.shards_per_id:
            self.shard_counter -= self.shards_per_id
        assert isinstance(element, JSONDict), 'element must be a JSONDict'
        # get timestamp from TimestampedValue
        yield ((element_id, shard), element)


class IdPartitionedFileSink(PartitionedFileSink):
    def _encode_key(self, id):
        """convert an id to a string representation"""
        return str(id)

