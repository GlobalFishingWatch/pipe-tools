import posixpath as pp
import itertools as it
from collections import defaultdict
import six
import uuid
import time
import logging

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import apache_beam.io.gcp.internal.clients.bigquery as bq

from pipe_tools.io import DatePartitionedFileSink
from pipe_tools.io import WriteToDatePartitionedFiles
from pipe_tools.io.bigquery import BigQueryWrapper
from pipe_tools.io.bigquery import parse_table_schema
from pipe_tools.io.bigquery import encode_table_ref
from pipe_tools.io.bigquery import decode_table_ref


DEFAULT_TEMP_SHARDS_PER_DAY=16


class WriteToBigQueryDatePartitioned(WriteToDatePartitionedFiles):
    """
    Write the incoming pcoll to a bigquery table partitioned by date.  The date is taken from the
    TimestampedValue associated with each element.

    """

    def __init__(self, temp_gcs_location, table, dataset=None, project=None, schema=None,
                 create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=BigQueryDisposition.WRITE_EMPTY,
                 test_client=None, temp_shards_per_day=None):

        super(WriteToBigQueryDatePartitioned, self).__init__(
            file_path_prefix='notused', shards_per_day=temp_shards_per_day,
        )

        self._sink = BigQueryDatePartitionedSink(temp_gcs_location, table, dataset=dataset,
                                                 project=project, schema=schema,
                                                 create_disposition=create_disposition,
                                                 write_disposition=write_disposition,
                                                 test_client=test_client,
                                                 temp_shards_per_day=temp_shards_per_day)


class BigQueryDatePartitionedSink(DatePartitionedFileSink):
    DATE_FORMAT='%Y%m%d'

    def __init__(self, temp_gcs_location, table, dataset=None, project=None, schema=None,
                 create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=BigQueryDisposition.WRITE_EMPTY,
                 test_client=None, temp_shards_per_day=0):

        self.temp_shards_per_day = temp_shards_per_day or DEFAULT_TEMP_SHARDS_PER_DAY

        # store table ref as a string so that it will pickle
        self.table_reference = decode_table_ref(table, dataset, project)
        self.project_id = project or self.table_reference.projectId

        # store schema as a dict so that it will pickle
        self.table_schema_dict = WriteToBigQuery.table_schema_to_dict(parse_table_schema(schema))
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition

        super(BigQueryDatePartitionedSink, self).__init__(
            file_path_prefix=pp.join(temp_gcs_location, 'shard'),
            file_name_suffix='.json'
        )

    @property
    def table_schema(self):
        return parse_table_schema(self.table_schema_dict)

    def date_sharded_table_ref(self, date_ts):

        table = "{}{}".format(self.table_reference.tableId, self._date_str(date_ts))
        return bq.TableReference(projectId=self.table_reference.projectId,
                                 datasetId=self.table_reference.datasetId,
                                 tableId=table)

    def _load_tables(self, client, shard_paths):

        # group the shard paths by date
        shards_by_date = defaultdict(list)
        for date_ts, shard in shard_paths:
            shards_by_date[date_ts].append(shard)

        for date_ts, shards in six.iteritems(shards_by_date):
            table_ref=self.date_sharded_table_ref(date_ts)
            job_id = client.load_table(
                job_id=uuid.uuid4().hex, project_id=self.project_id,
                table_ref=table_ref,
                table_schema=self.table_schema, gcs_urls=shards,
                create_disposition=self.create_disposition,
                write_disposition=self.write_disposition
            )
            yield (job_id, table_ref, date_ts)


    def finalize_write(self, init_result, writer_results):
        # writer_results is LIST[ LIST[ TUPLE[date_ts, gcs_path] ] ]
        # so first we need to flatten it to just LIST[ TUPLE[date_ts, gcs_path] ]
        shard_paths = it.chain.from_iterable(writer_results)

        client = BigQueryWrapper()

        # start an async load table job for each date
        waiting_jobs=set(self._load_tables(client, shard_paths))

        # wait for jobs to finish
        while waiting_jobs:
            logging.info('Waiting for %s bigquery tables to load...', len(waiting_jobs))
            completed_jobs = set()
            for job in waiting_jobs:
                job_id, table_ref, date_ts = job
                table_str = encode_table_ref(table_ref, True)
                response = client.get_job_status(self.project_id, job_id)
                if response.status.state == "DONE":
                    completed_jobs.add(job)
                    if response.status.errorResult:
                        logging.error('Bigquery table load failed for %s', table_str)
                        for error in response.status.errors:
                            logging.error ('%s %s %s', error.reason, error.location, error.message)

                        # raise exception
                        raise RuntimeError('Bigquery table load failed for table %s.  %s' % (table_str, response.status.errorResult.message))
                    else:
                        logging.info('Bigquery table load complete for %s', table_str)
                        yield table_str     # not sure what anyone is going to do with these...
                else:
                    #  Not done yet...
                    logging.debug('Bigquery table load status %s - %s' % (table_str, response.status.state))

            waiting_jobs -= completed_jobs
            time.sleep(1.0) # wait for a bit and then check again
            continue

        try:
            FileSystems.delete([init_result])
        except IOError:
            # May have already been removed.
            pass
