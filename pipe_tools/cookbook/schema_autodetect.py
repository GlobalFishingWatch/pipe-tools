import logging
import warnings
import sys
from pipe_tools.beam import logging_monkeypatch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, TypeOptions, GoogleCloudOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import BigQuerySource
from apache_beam.io.gcp.bigquery import BigQuerySink

from apitools.base.py.exceptions import HttpError

from pipe_tools.generator import GenerateMessages
from pipe_tools.generator import MessageGenerator
from pipe_tools.io.bigquery import BigQueryWrapper
from pipe_tools.io.bigquery import decode_table_ref
from pipe_tools.coders import ReadAsJSONDict
from pipe_tools.transforms.util import DoNothing

# Bigquery Schema Autodetect
#
# Demonstrates how to detect the schema of a source table and use it to create an output
# table with the same schema.  Of course, you could add some fields to the schema along the way...
#
# Usage:
#
# python schema_autodetect.py --help
# python schema_autodetect.py \
#     --runner=DirectRunner \
#     --source-table=PROJECT:DATASET.TABLE \
#     --dest_table=PROJECT:DATASET.TABLE_TO_CREATE
#     --project=PROJECT \
#
# python schema_autodetect.py \
#     --runner=DataflowRunner \
#     --setup_file =../../ setup.py \
#     --source-table=PROJECT:DATASET.TABLE \
#     --dest_table=PROJECT:DATASET.TABLE_TO_CREATE
#     --project=PROJECT \
#     --temp_location=gs://BUCKET/dataflow-temp/ \
#     --staging_location=gs://BUCKET/dataflow-staging/ \
#     --wait



class MyPipelineOptions(StandardOptions, TypeOptions, GoogleCloudOptions):
    DEFAULT_COUNT=10

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--source-table',
            help='Read records from this source table. And use the schema from this table. Required',
            required=True
        )
        parser.add_argument(
            '--dest-table',
            help='Create this table with the same schema as the source table and write records to it. Required',
            required=True
        )
        parser.add_argument(
            '--wait',
            help='When present, waits until the dataflow job is done before exiting.',
            action='store_true',
            default=False,
        )


def detect_schema(table):
    client = BigQueryWrapper()
    table_ref = decode_table_ref(table)
    try:
        return client.get_table_schema(table_ref.projectId, table_ref.datasetId, table_ref.tableId)
    except HttpError as exn:
        if exn.status_code == 404:
            logging.error('Table %s not found.' % table)
        else:
            raise


# build a test pipeline to read from bq, detecth the schema, and write to a new bq table
def build_pipeline(options):

    detected_schema = detect_schema(options.source_table)
    source = BigQuerySource(table=options.source_table)
    sink = BigQuerySink(table=options.dest_table, schema=detected_schema)

    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "ReadFromBigQuery" >> beam.io.Read(source)
        | "DoSomething" >> DoNothing()
        | "WriteToBigQuery" >> beam.io.Write(sink)
    )

    return pipeline


# parse commandline args, build and run a pipeline
def run(args=None):
    # if no arguments given, display usage
    args = args or ['--help']
    options = MyPipelineOptions(args)

    print "Reading schema from {} ...".format(options.source_table)
    pipeline = build_pipeline(options)
    result = pipeline.run()
    if options.wait:
        result.wait_until_finish()

    return 0 if result.state == 'DONE' else 1

# call run() with the args from the command line
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    result = run(args=sys.argv[1:])
    sys.exit(result)

