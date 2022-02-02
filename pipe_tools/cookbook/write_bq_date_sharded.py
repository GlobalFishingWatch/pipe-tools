import logging
import warnings
import sys
from pipe_tools.beam import logging_monkeypatch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, TypeOptions, GoogleCloudOptions
from apache_beam.io import WriteToText

from pipe_tools.generator import GenerateMessages
from pipe_tools.generator import MessageGenerator
from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.io import WriteToBigQueryDateSharded


# Write to Date Partitioned Files
#
# Demonstrates how to write to date-partitioned files using WriteToDatePartitionedFiles
# NOTE:  So far all this does is write date SHARDED tables with one table per day
# Some work needs to be done yet to write to true date partitioned tables.
#
# Usage:
#
# python write_bq_date_partitions.py --help
# python write_bq_date_partitions.py --output-table=project:dataset.table
# python write_bq_date_partitions.py --count=1000 --output-table=project:dataset.table
# cp write_bq_date_partitions_args.txt myargs.txt
# nano myargs.txt
# python write_bq_date_partitions.py $(cat myargs.txt)
#

class MyPipelineOptions(StandardOptions, TypeOptions, GoogleCloudOptions):
    DEFAULT_COUNT=72

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--count',
                            help='number of messages to generate. default %s' % cls.DEFAULT_COUNT,
                            default=cls.DEFAULT_COUNT
        )
        parser.add_argument('--output-table',
                            help='bigquery table destination.  Required',
                            required=True
                            )


# build a test pipeline to generate messages and write to a file
def build_pipeline(options):

    pipeline = beam.Pipeline(options=options)
    temp_gcs_location = options.view_as(GoogleCloudOptions).temp_location

    # generate some messages to write
    generator = MessageGenerator(count=int(options.count))
    schema=generator.bigquery_schema()
    messages = pipeline | "GenerateMessages" >> GenerateMessages(generator=generator)

    # date partitioning is based on the timestamp used for windowing
    messages = messages | "AddTimestampedValue" >> beam.ParDo(TimestampedValueDoFn())

    messages | "WriteDateSharded" >> WriteToBigQueryDateSharded(
        temp_gcs_location=temp_gcs_location,
        table=options.output_table,
        schema=schema,
        temp_shards_per_day=1)

    return pipeline


# parse commandline args, build and run a pipeline
def run(args=None):
    # if no arguments given, display usage
    args = args or ['--help']
    options = MyPipelineOptions(args)
    print("Writing {} messages to {}".format(options.count, options.output_table))

    pipeline = build_pipeline(options)
    job = pipeline.run()


# call run() with the args from the command line
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(args=sys.argv[1:])

