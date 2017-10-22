import ujson
import uuid

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import apache_beam.io.gcp.internal.clients.bigquery as bq
from apache_beam.utils import retry
from apache_beam.io.gcp.bigquery import _parse_table_reference


def parse_table_schema(schema):
    """
    Accepts a BigQuery tableschema as a string, dict (from json), or bigquery.TabelSchema and returns
    a bigquery.TableSchema

    String Format

    "[FIELD]:[DATA_TYPE],[FIELD]:[DATA_TYPE]"

    dict format

    {
      "fields": [
        {
          "name": "[FIELD]",
          "type": "[DATA_TYPE]"
        },
        {
          "name": "[FIELD]",
          "type": "[DATA_TYPE]"
        }
    ]}

    see https://cloud.google.com/bigquery/data-types
    see https://cloud.google.com/bigquery/docs/schemas#specifying_a_schema_file


    """
    if schema is None:
        return schema
    elif isinstance(schema, bq.TableSchema):
        return schema
    elif isinstance(schema, basestring):
        # try to parse json into dict
        try:
            schema = ujson.loads(schema)
        except ValueError, e:
            pass

    if isinstance(schema, basestring):
        # if it is still a string, then it must not be json.  Assume it is string representation
        return WriteToBigQuery.get_table_schema_from_string(schema)
    elif isinstance(schema, dict):
        # either it came in as a dict or it got converted from json earlier
        return parse_table_schema_from_json(ujson.dumps(schema))
    else:
        raise TypeError('Unexpected schema argument: %s.' % schema)


# decode a bigquery table ref string PROJECT:DATASET.TABLE_ID
# into a TableReferenece.   You can supply each component separately,
# or all in one string
def decode_table_ref(table, dataset=None, project=None):
    return _parse_table_reference(table, dataset, project)


# encode a TableReference to a string representation
def encode_table_ref(table_ref):
    if table_ref.projectId:
        return "{}:{}.{}".format(table_ref.projectId, table_ref.datasetId, table_ref.tableId)
    else:
        return "{}.{}".format(table_ref.datasetId, table_ref.tableId)



# subclass BigQueryWrapper so we can add a few things
class BigQueryWrapper(beam.io.gcp.bigquery.BigQueryWrapper):
    def __init__(self, **kwargs):
        super(BigQueryWrapper, self).__init__(**kwargs)


    @retry.with_exponential_backoff(
        num_retries=beam.io.gcp.bigquery.MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def load_table(self, job_id, project_id, table_ref, table_schema, gcs_urls,
                   create_disposition, write_disposition):

        job_ref = bq.JobReference(jobId=job_id, projectId=project_id)
        request = bq.BigqueryJobsInsertRequest(
            projectId=project_id,
            job=bq.Job(
                configuration=bq.JobConfiguration(
                    load=bq.JobConfigurationLoad(
                        createDisposition=create_disposition,
                        destinationTable=table_ref,
                        schema=table_schema,
                        sourceFormat="NEWLINE_DELIMITED_JSON",
                        sourceUris=gcs_urls,
                        writeDisposition=write_disposition
                    )),
                jobReference=job_ref
            ))

        response = self.client.jobs.Insert(request)
        return response.jobReference.jobId


    @retry.with_exponential_backoff(
        num_retries=beam.io.gcp.bigquery.MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_job_status(self, project_id, job_id):
        request = bq.BigqueryJobsGetRequest(
            jobId=job_id, projectId=project_id)
        return self.client.jobs.Get(request)

