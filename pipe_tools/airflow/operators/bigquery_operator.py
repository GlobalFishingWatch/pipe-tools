import json

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook, _parse_gcs_url
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryCreateEmptyTableOperator(BaseOperator):
    """
    Creates a new, empty table in the specified BigQuery dataset,
    optionally with schema if the table doesn't exit.
    
    This is a hack that we had to implement due to the fact that
    Dataflow BQ Sink does not have the create disposition `ALWAYS_CREATE`

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google cloud storage object name. The object in
    Google cloud storage must be a JSON file with the schema fields in it.
    You can also create a table without schema.

    :param project_id: The project to create the table into. (templated)
    :type project_id: string
    :param dataset_id: The dataset to create the table into. (templated)
    :type dataset_id: string
    :param table_id: The Name of the table to be created. (templated)
    :type table_id: string
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

    :type schema_fields: list
    :param gcs_schema_object: Full path to the JSON file containing
        schema (templated). For
        example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
    :type gcs_schema_object: string
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
    :type time_partitioning: dict
    :param bigquery_conn_id: Reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string

    **Example (with schema JSON in GCS)**: ::

        CreateTable = BigQueryCreateEmptyTableOperator(
            task_id='BigQueryCreateEmptyTableOperator_task',
            dataset_id='ODS',
            table_id='Employees',
            project_id='internal-gcp-project',
            gcs_schema_object='gs://schema-bucket/employee_schema.json',
            bigquery_conn_id='airflow-service-account',
            google_cloud_storage_conn_id='airflow-service-account'
        )

    **Corresponding Schema file** (``employee_schema.json``): ::

        [
          {
            "mode": "NULLABLE",
            "name": "emp_name",
            "type": "STRING"
          },
          {
            "mode": "REQUIRED",
            "name": "salary",
            "type": "INTEGER"
          }
        ]

    **Example (with schema in the DAG)**: ::

        CreateTable = BigQueryCreateEmptyTableOperator(
            task_id='BigQueryCreateEmptyTableOperator_task',
            dataset_id='ODS',
            table_id='Employees',
            project_id='internal-gcp-project',
            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}],
            bigquery_conn_id='airflow-service-account',
            google_cloud_storage_conn_id='airflow-service-account'
        )

    """
    template_fields = ('dataset_id', 'table_id', 'project_id', 'gcs_schema_object')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 project_id=None,
                 schema_fields=None,
                 gcs_schema_object=None,
                 time_partitioning={},
                 bigquery_conn_id='bigquery_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):

        super(BigQueryCreateEmptyTableOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = time_partitioning

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)
        
        table_exists =  bq_hook.table_exists(self.project_id,
                                             self.dataset_id,
                                             self.table_id)
        if not table_exists:
          if not self.schema_fields and self.gcs_schema_object:
  
              gcs_bucket, gcs_object = _parse_gcs_url(self.gcs_schema_object)
  
              gcs_hook = GoogleCloudStorageHook(
                  google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                  delegate_to=self.delegate_to)
              schema_fields = json.loads(gcs_hook.download(
                  gcs_bucket,
                  gcs_object).decode("utf-8"))
          else:
              schema_fields = self.schema_fields
  
          conn = bq_hook.get_conn()
          cursor = conn.cursor()
  
          cursor.create_empty_table(
              project_id=self.project_id,
              dataset_id=self.dataset_id,
              table_id=self.table_id,
              schema_fields=schema_fields,
              time_partitioning=self.time_partitioning
          )