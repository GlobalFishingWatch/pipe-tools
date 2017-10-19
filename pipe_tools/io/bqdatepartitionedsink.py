import posixpath as pp

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.transforms import window
from apache_beam import core
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from pipe_tools.io import DatePartitionedFileSink
from pipe_tools.io import DatePartitionedFileSinkWriter
from pipe_tools.io.datepartitionedsink import DateShardDoFn
from pipe_tools.coders import JSONDictCoder

DEFAULT_TEMP_SHARDS_PER_DAY=16


class WriteToBigQueryDatePartitioned(PTransform):
    """
    Write the incoming pcoll to a bigquery table partitioned by date.  The date is taken from the
    TimestampedValue associated with each element.

    """

    def __init__(self, temp_gcs_location, table, dataset=None, project=None, schema=None,
                 create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=BigQueryDisposition.WRITE_APPEND,
                 test_client=None, temp_shards_per_day=0):

        self._sink = BigQueryDatePartitionedSink(temp_gcs_location. table, dataset=dataset,
                                                 project=project, schema=schema,
                                                 create_disposition=create_disposition,
                                                 write_disposition=write_disposition,
                                                 test_client=test_client,
                                                 temp_shards_per_day=temp_shards_per_day)

    def expand(self, pcoll):
        return (
            pcoll
            | core.WindowInto(window.GlobalWindows())
            | beam.ParDo(DateShardDoFn(shards_per_day=self.shards_per_day))
            | beam.GroupByKey()   # group by day and shard
            | beam.io.Write(self._sink)
        )


class BigQueryDatePartitionedSink(DatePartitionedFileSink):
    def __init__(self, temp_gcs_location, table, dataset=None, project=None, schema=None,
                 create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=BigQueryDisposition.WRITE_APPEND,
                 test_client=None, temp_shards_per_day=0):

        self.temp_shards_per_day = temp_shards_per_day or DEFAULT_TEMP_SHARDS_PER_DAY
        self.table = table
        self.dataset = dateset
        self.project = project
        self.table_schema_dict = io.gcp.bigquery.WriteToBigQuery.table_schema_to_dict(parse_table_schema(schema))

        super(BigQueryDatePartitionedSink, self).__init__(
            file_path_prefix=pp.join(temp_gcs_location, 'shard'),
            file_name_suffix='.json',
            append_trailing_newlines=True,
            num_shards=self.temp_shards_per_day,
            coder=JSONDictCoder()
        )


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

