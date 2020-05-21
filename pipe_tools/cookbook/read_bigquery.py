import logging
import sys
from pipe_tools.beam import logging_monkeypatch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, TypeOptions, GoogleCloudOptions

from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.io import WriteToDatePartitionedFiles
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn
from pipe_tools.options import ReadFileAction

# Read from Bigquery and write to Date Partitioned Files
#
# Demonstrates how to read AIS/VMS messages from bigquery and write to date-partitioned files
#
# Usage:
#
#   python read_bigquery.py --help
#   cp read_bigquery_args_local.txt ./myargs.txt
#   nano myargs.txt
#   python read_bigquery.py $(cat myargs.txt)
#
#


class MyPipelineOptions(StandardOptions, TypeOptions, GoogleCloudOptions):
    DEFAULT_SHARDS_PER_DAY=1
    DEFAULT_FILE_SUFFIX='.json'

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--output-file-prefix',
                            help='prefix for the output file to write.  Required',
                            required=True
                            )
        parser.add_argument('--output-file-suffix',
                            help='prefix for the output file to write.  Default %s' % cls.DEFAULT_FILE_SUFFIX,
                            default=cls.DEFAULT_FILE_SUFFIX
                            )
        parser.add_argument('--shards-per-day',
                            help='Number of file shards to write for each day.  Default %s' % cls.DEFAULT_SHARDS_PER_DAY,
                            default=cls.DEFAULT_SHARDS_PER_DAY
                            )
        parser.add_argument('--query',
                            help='Query to run.  Use @file.sql to read from a file.  Required',
                            required=True,
                            action=ReadFileAction
                            )
        parser.add_argument('--wait',
                            help='When present, waits until the dataflow job is done before returning.',
                            action='store_true',
                            default=False,
        )



# build a test pipeline to generate messages and write to a file
def build_pipeline(options):

    source = beam.io.gcp.bigquery.BigQuerySource(query=options.query)

    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "ReadFromBigQuery" >> beam.io.Read(source)
        | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn())
        | "AddTimestampedValue" >> beam.ParDo(TimestampedValueDoFn())
        | "WriteDatePartitions" >> WriteToDatePartitionedFiles(file_path_prefix=options.output_file_prefix,
                                                                    file_name_suffix=options.output_file_suffix,
                                                                    shards_per_day=options.shards_per_day)
    )

    return pipeline


# parse commandline args, build and run a pipeline
def run(args=None):
    # if no arguments given, display usage
    args = args or ['--help']
    options = MyPipelineOptions(args)
    print("Writing from bigquery and writing to  {}".format(options.output_file_prefix))

    pipeline = build_pipeline(options)
    job = pipeline.run()

    if options.wait:
        job.wait_until_finish()


# call run() with the args from the command line
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(args=sys.argv[1:])

