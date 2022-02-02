import logging
import sys
from pipe_tools.beam import logging_monkeypatch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, TypeOptions, GoogleCloudOptions

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.io import WriteToBigQueryDateSharded
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn
from pipe_tools.options import ReadFileAction

# Read from Bigquery and write to BigQuery Date Partitioned tables
#
# Usage:
#
# python read_write_bigquery.py \
#     --runner=DirectRunner \
#     --output-table=${PROJECT}:${DATASET}.read_write_bigquery_ \
#     --project=${PROJECT} \
#     --temp_location=gs://${BUCKET}/dataflow-temp/ \
#     --staging_location=gs://${BUCKET}/dataflow-staging/ \
#     --query=@query-8k-rows-2-fields.sql \
#     --schema=mmsi:INTEGER,timestamp:TIMESTAMP


class MyPipelineOptions(StandardOptions, TypeOptions, GoogleCloudOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--query',
            help='Query to run for input.  Use "@file.sql" to read the query from a file.  Required',
            required=True,
            action=ReadFileAction
        )
        parser.add_argument(
            '--schema',
            help='Bigquery schema for the output table.  Should be the same as the input query.'
                 'This may be in the form field:type,field:type or encoded as json.  Use'
                 '@file.schema to load this fromm a file.  Required',
            required=True,
            action=ReadFileAction
        )
        parser.add_argument(
            '--output-table',
            help='bigquery table destination.  Required',
            required=True
        )
        parser.add_argument(
            '--wait',
            help='When present, waits until the dataflow job is done before exiting.',
            action='store_true',
            default=False,
        )


# build a test pipeline to run the query and write to bigquery
def build_pipeline(options):

    source = beam.io.gcp.bigquery.BigQuerySource(query=options.query)
    temp_gcs_location = options.view_as(GoogleCloudOptions).temp_location

    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "ReadFromBigQuery" >> beam.io.Read(source)
        | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn())
        | "AddTimestampedValue" >> beam.ParDo(TimestampedValueDoFn())
        | "WriteDateSharded" >> WriteToBigQueryDateSharded(
            temp_gcs_location=temp_gcs_location,
            table=options.output_table,
            schema=options.schema,
            temp_shards_per_day=3)
    )

    return pipeline


# parse commandline args, build and run a pipeline
def run(args=None):
    # if no arguments given, display usage
    args = args or ['--help']
    options = MyPipelineOptions(args)
    print("Writing from bigquery and writing to {}".format(options.output_table))

    pipeline = build_pipeline(options)
    result = pipeline.run()

    if options.wait:
        result.wait_until_finish()

    return 0 if result.state == 'DONE' else 1

# call run() with the args from the command line
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    result = run(args=sys.argv[1:])
    sys.exit(result)

