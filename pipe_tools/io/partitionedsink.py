import random
import posixpath as pp
import itertools as it
from collections import defaultdict
import time
import logging
import warnings
import six

import apache_beam as beam
from apache_beam.io.filebasedsink import FileBasedSink
from apache_beam.io.filebasedsink import FileBasedSinkWriter
from apache_beam import PTransform
from apache_beam import core
from apache_beam.transforms import window
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.filesystem import BeamIOError
from apache_beam.internal import util

from pipe_tools.timestamp import SECONDS_IN_DAY
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.coders import JSONDictCoder
from pipe_tools.coders import JSONDict
from apache_beam import typehints
from apache_beam.typehints import Tuple, KV

T = typehints.TypeVariable('T')


@typehints.with_output_types(str)
class WritePartitionedFiles(PTransform):
    """
    Write the incoming pcoll to partioned files. Must be subclassed
    and __init__ implements. The attributes:

        _sink: a subclass of PartitionedFileSink
        _sharder: a do-function that produces ((key, shard), value) tuples

    must be created in the __init__. See datepartionsink.py and keypartitionsink.py
    for examples.

    """

    def expand(self, pcoll):
        pcoll = (
            pcoll
            | core.WindowInto(window.GlobalWindows())
            | beam.ParDo(self._sharder)
            | beam.GroupByKey()   # group by id and shard
        )
        with warnings.catch_warnings():
            # suppress a spurious warning generated within beam.io.Write.  This warning is annoying but harmless
            warnings.filterwarnings(action="ignore", message="Using fallback coder for typehint: <type 'NoneType'>")

            return pcoll | beam.io.Write(self._sink).with_output_types(str)


class PartitionedFileSink(FileBasedSink):

    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='',
                 append_trailing_newlines=True,
                 shard_name_template=None,
                 coder=JSONDictCoder(),
                 compression_type=CompressionTypes.AUTO,
                 header=None):
        self._do_sharding = not (shard_name_template == '')
        super(PartitionedFileSink, self).__init__(
            file_path_prefix,
            file_name_suffix=file_name_suffix,
            num_shards=0,
            shard_name_template=None,
            coder=coder,
            mime_type='text/plain',
            compression_type=compression_type)
        self._append_trailing_newlines = append_trailing_newlines
        self._header = header


    def _encode_key(self, key):
        raise NotImplementedError()

    def get_temp_shard_path(self, temp_file_prefix, key, shard):
        raise NotImplementedError()

    def open(self, temp_path):
        file_handle = super(PartitionedFileSink, self).open(temp_path)
        if self._header is not None:
            file_handle.write(self._header)
            if self._append_trailing_newlines:
                file_handle.write('\n')
        return file_handle

    def display_data(self):
        dd_parent = super(PartitionedFileSink, self).display_data()
        dd_parent['append_newline'] = DisplayDataItem(
            self._append_trailing_newlines,
            label='Append Trailing New Lines')
        return dd_parent

    def write_encoded_record(self, file_handle, encoded_value):
        """Writes a single encoded record."""
        file_handle.write(encoded_value)
        if self._append_trailing_newlines:
            file_handle.write('\n')

    def open_writer(self, init_result, uid):
        return PartitionedFileSinkWriter(self, pp.join(init_result, uid))

    def get_temp_shard_path(self, temp_file_prefix, key, shard):
        key_str = self._encode_key(key)
        file_path_prefix = self.file_path_prefix.get()
        file_name_suffix = self.file_name_suffix.get()
        return '%s_%s_%s.%s%s' % (temp_file_prefix, key_str, shard,
                                  pp.basename(file_path_prefix), file_name_suffix)

    def _source_dest_shard_pairs(self, shard_paths):
        """
        generator that take a list of (key, shard_path:string) pairs and
        yields a stream of (source:string, dest:string) pairs

        where the input is a shard key and full path to a temporary shard file
        and the output is the temp shard file as source and the matching
        destination shard file path as dest
        """
        def key_fn(k):
            return k[0]

        file_path_prefix = self.file_path_prefix.get()
        file_name_suffix = self.file_name_suffix.get()

        # split the file_path_prefix into path and filename components.  If we are sharding,
        # we will insert a directory for each key. Otherwise, we append sharding key to prefix.
        file_path, file_name_prefix = pp.split(file_path_prefix)

        # first group the shard paths by key
        shards_by_key = defaultdict(list)
        for key, shard in shard_paths:
            shards_by_key[key].append(shard)

        for key, shards in six.iteritems(shards_by_key):
            encoded_key = self._encode_key(key)
            if self._do_sharding:
                dest_path = pp.join(file_path, encoded_key)

                shards = sorted(shards)
                num_shards = len(shards)
                for shard_num, source_shard in enumerate(shards):
                    dest_file_name = ''.join([
                            file_name_prefix, self.shard_name_format % dict(
                                shard_num=shard_num, num_shards=num_shards), file_name_suffix
                        ])

                    dest_shard = pp.join(dest_path, dest_file_name)
                    yield (source_shard, dest_shard)
            else:
                dest_path = file_path
                [source_shard] = shards

                dest_file_name = ''.join([
                            file_name_prefix, encoded_key, file_name_suffix
                    ])
                dest_shard = pp.join(dest_path, dest_file_name)

                yield (source_shard, dest_shard)


    # Use a thread pool for creating dirs.
    @staticmethod
    def _create_output_dir(path):
        try:
            FileSystems.mkdirs(path)
        except IOError:
            # we expect IOError if the dir already exists
            pass

    # Use a thread pool for renaming operations.
    @staticmethod
    def _rename_batch(batch):
        """_rename_batch executes batch rename operations."""

        source_files, destination_files = zip(*batch)
        exceptions = []
        try:
            FileSystems.rename(source_files, destination_files)
            return exceptions
        except BeamIOError as exp:
            if exp.exception_details is None:
                raise
            for (src, dest), exception in exp.exception_details.iteritems():
                if exception:
                    logging.warning('Rename not successful: %s -> %s, %s', src, dest,
                                    exception)
                    should_report = True
                    if isinstance(exception, IOError):
                        # May have already been copied.
                        try:
                            if FileSystems.exists(dest):
                                should_report = False
                        except Exception as exists_e:  # pylint: disable=broad-except
                            logging.warning('Exception when checking if file %s exists: '
                                            '%s', dest, exists_e)
                    if should_report:
                        logging.warning(('Exception in _rename_batch. src: %s, '
                                         'dest: %s, err: %s'), src, dest, exception)
                        exceptions.append(exception)
                else:
                    logging.debug('Rename successful: %s -> %s', src, dest)
            return exceptions

    def finalize_write(self, init_result, writer_results, pre_finalize_result):
        file_path_prefix = self.file_path_prefix.get()

        shard_paths = it.chain.from_iterable(writer_results)
        path_pairs = list(self._source_dest_shard_pairs(shard_paths))
        unique_dest_dirs = {pp.split(pair[1])[0] for pair in path_pairs}

        num_shards = len(path_pairs)
        min_threads = min(num_shards, FileBasedSink._MAX_RENAME_THREADS)
        num_threads = max(1, min_threads)

        batch_size = FileSystems.get_chunk_size(file_path_prefix)
        batches = [path_pairs[i:i + batch_size] for i in 
                        six.moves.range(0, len(path_pairs), batch_size)]

        logging.info(
            'Starting finalize_write threads with num_shards: %d, '
            'batches: %d, num_threads: %d',
            num_shards, len(batches), num_threads)
        start_time = time.time()

        if unique_dest_dirs:
            # Fix #18 run_using_threadpool raises if you pass in an empty list of inputs
            # so if we don't have any work to do, then just skip it
            util.run_using_threadpool(self._create_output_dir, unique_dest_dirs, num_threads)

            exception_batches = util.run_using_threadpool(self._rename_batch, batches, num_threads)

            all_exceptions = [e for exception_batch in exception_batches for e in exception_batch]

            if all_exceptions:
                raise Exception('Encountered exceptions in finalize_write: %s',
                                all_exceptions)

        for _, final_name in path_pairs:
            yield final_name

        logging.info('Renamed %d shards in %.2f seconds.', num_shards,
                     time.time() - start_time)

        try:
            FileSystems.delete([init_result])
        except IOError:
            # May have already been removed.
            pass


class PartitionedFileSinkWriter(FileBasedSinkWriter):
    """The writer for PartitionedFileSink.
    """

    def __init__(self, sink, temp_file_prefix):
        self.sink = sink
        self.shard_paths = []
        self.temp_file_prefix = temp_file_prefix

    def write(self, value):
        # value is created by ShardDoFn.process(), so it contains a partition key
        # and it is grouped by key, so it contains a stream of rows to write
        # Because we are grouped by shard, all the rows for a particular shard come in a single
        # call to write().  Therefore we open, write and close all in one go

        (partition_key, shard), rows = value
        temp_shard_path = self.sink.get_temp_shard_path(self.temp_file_prefix, partition_key, shard)
        temp_handle = self.sink.open(temp_shard_path)
        for row in rows:
            self.sink.write_record(temp_handle, row)
        self.sink.close(temp_handle)
        self.shard_paths.append((partition_key, temp_shard_path))

    def close(self):
        return self.shard_paths     # this is a list of (partition_key, path) tuples

