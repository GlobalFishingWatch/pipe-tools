import random
import posixpath as pp
import itertools as it
from collections import defaultdict
import time
import logging
import six

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.io.filebasedsink import FileBasedSink
from apache_beam.io.filebasedsink import FileBasedSinkWriter
from apache_beam.coders import TimestampCoder
from apache_beam import core
from apache_beam import window
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.filesystem import BeamIOError
from apache_beam.internal import util



from pipe_tools.timestamp import SECONDS_IN_DAY
from pipe_tools.timestamp import datetimeFromTimestamp
from pipe_tools.coders import JSONCoder

class WriteToDatePartitionedFiles(PTransform):
    """
    Write the incoming pcoll to files partitioned by date.  The date is take from the
    TimestampedValue associated wth each element.

    """
    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='',
                 append_trailing_newlines=True,
                 shards_per_day=1,
                 shard_name_template=None,
                 coder=JSONCoder(),
                 compression_type=CompressionTypes.AUTO,
                 header=None):

        self.shards_per_day = shards_per_day

        self._sink = DatePartitionedFileSink(file_path_prefix,
                                             file_name_suffix=file_name_suffix,
                                             append_trailing_newlines=append_trailing_newlines,
                                             num_shards=1,
                                             shard_name_template=shard_name_template,
                                             coder=coder,
                                             compression_type=compression_type,
                                             header=header)

    def expand(self, pcoll):
        return (
            pcoll
            | core.WindowInto(window.GlobalWindows())
            | beam.ParDo(DateShardDoFn(shards_per_day=self.shards_per_day))
            | beam.GroupByKey()   # group by day and shard
            | beam.io.Write(self._sink)
        )


class DateShardDoFn(beam.DoFn):
    """
    Apply date and shard number
    """

    def __init__(self, shards_per_day=1):
        assert shards_per_day > 0
        self.shards_per_day = shards_per_day
        self.shard_counter = 0

    def start_bundle(self):
        self.shard_counter = random.randint(0, self.shards_per_day - 1)

    def process(self, element, timestamp=beam.DoFn.TimestampParam):

        # get the timestamp at the start of the day that contains this element
        date = (int(timestamp) / SECONDS_IN_DAY) * SECONDS_IN_DAY
        shard = self.shard_counter

        self.shard_counter += 1
        if self.shard_counter >= self.shards_per_day:
            self.shard_counter -= self.shards_per_day

        # get timestamp from TimestampedValue
        yield ((date, shard), element)


class DatePartitionedFileSink(FileBasedSink):
    DATE_FORMAT='%Y-%m-%d'

    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='',
                 append_trailing_newlines=True,
                 num_shards=0,
                 shard_name_template=None,
                 coder=JSONCoder(),
                 compression_type=CompressionTypes.AUTO,
                 header=None):

        super(DatePartitionedFileSink, self).__init__(
            file_path_prefix,
            file_name_suffix=file_name_suffix,
            num_shards=num_shards,
            shard_name_template=shard_name_template,
            coder=coder,
            mime_type='text/plain',
            compression_type=compression_type)
        self._append_trailing_newlines = append_trailing_newlines
        self._header = header

    def _date_str(self, date_ts):
        """convert a timestamp to a string date representation"""
        return datetimeFromTimestamp(date_ts).strftime(self.DATE_FORMAT)

    def open(self, temp_path):
        file_handle = super(DatePartitionedFileSink, self).open(temp_path)
        if self._header is not None:
            file_handle.write(self._header)
            if self._append_trailing_newlines:
                file_handle.write('\n')
        return file_handle

    def display_data(self):
        dd_parent = super(DatePartitionedFileSink, self).display_data()
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
        return DatePartitionedFileSinkWriter(self, pp.join(init_result, uid))

    def get_temp_shard_path(self, temp_file_prefix, date_ts, shard):
        date_str = datetimeFromTimestamp(date_ts).strftime(self.DATE_FORMAT)
        file_path_prefix = self.file_path_prefix.get()
        file_name_suffix = self.file_name_suffix.get()
        return '%s_%s_%s.%s%s' % (temp_file_prefix, date_str, shard,
                                  pp.basename(file_path_prefix), file_name_suffix)

    def _source_dest_shard_pairs(self, shard_paths):
        """
        generator that take a list of (date_ts:timestamp, shard_path:string) pairs and
        yields a stream of (source:string, dest:string) pairs

        where the input is a timestamp and full patch to a temporary shard file
        and the output is the temp shard file as source and the matching
        file shard file path as dest
        """
        def key_fn(k):
            return k[0]

        file_path_prefix = self.file_path_prefix.get()
        file_name_suffix = self.file_name_suffix.get()

        # split the file_path_prefix into path and filename componenets.  We will insert
        # a directory for each day

        file_path, file_name_prefix = pp.split(file_path_prefix)

        # first group the shard paths by date
        shards_by_date = defaultdict(list)
        for date_ts, shard in shard_paths:
            shards_by_date[date_ts].append(shard)

        for date_ts, shards in six.iteritems(shards_by_date):
            dest_path = pp.join(file_path, self._date_str(date_ts))
            shards = sorted(shards)
            num_shards = len(shards)

            for shard_num, source_shard in enumerate(shards):
                dest_file_name = ''.join([
                    file_name_prefix, self.shard_name_format % dict(
                        shard_num=shard_num, num_shards=num_shards), file_name_suffix
                ])
                dest_shard = pp.join(dest_path, dest_file_name)
                yield (source_shard, dest_shard)

    # Use a thread pool for renaming operations.
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

    def finalize_write(self, init_result, writer_results):
        file_path_prefix = self.file_path_prefix.get()

        shard_paths = it.chain.from_iterable(writer_results)
        path_pairs = list(self._source_dest_shard_pairs(shard_paths))
        unique_dest_dirs = {pp.split(pair[1])[0] for pair in path_pairs}

        num_shards = len(path_pairs)
        min_threads = min(num_shards, FileBasedSink._MAX_RENAME_THREADS)
        num_threads = max(1, min_threads)

        batch_size = FileSystems.get_chunk_size(file_path_prefix)
        batches = [path_pairs[i:i + batch_size] for i in xrange(0, len(path_pairs), batch_size)]

        logging.info(
            'Starting finalize_write threads with num_shards: %d, '
            'batches: %d, num_threads: %d',
            num_shards, len(batches), num_threads)
        start_time = time.time()

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


class DatePartitionedFileSinkWriter(FileBasedSinkWriter):
    """The writer for DatePartitionedFileSink.
    """

    def __init__(self, sink, temp_file_prefix):
        self.sink = sink
        self.shard_paths = []
        self.temp_file_prefix = temp_file_prefix

    def write(self, value):
        # value is created by DateShardDoFn.process(), so it contains a key with date and shard number
        # and it is grouped by key, so it contains a stream of rows to write
        # because we are grouped by shard, all the rows for a particular shard come in a single
        # call to write().  Therefore we open, write and close all in one go

        (date_ts, shard), rows = value
        temp_shard_path = self.sink.get_temp_shard_path(self.temp_file_prefix, date_ts, shard)
        temp_handle = self.sink.open(temp_shard_path)
        for row in rows:
            self.sink.write_record(temp_handle, row)
        self.sink.close(temp_handle)
        self.shard_paths.append((date_ts, temp_shard_path))

    def close(self):
        return self.shard_paths     # this is a list of (date_ts, path) tuples

