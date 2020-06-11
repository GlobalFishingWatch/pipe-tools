import logging
import warnings
import sys
from pipe_tools.beam import logging_monkeypatch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, TypeOptions
from apache_beam.io import WriteToText

from pipe_tools.generator import GenerateMessages
from pipe_tools.generator import MessageGenerator
from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.io import WriteToDatePartitionedFiles


# Write to Date Partitioned Files
#
# Demonstrates how to write to date-partitioned files using WriteToDatePartitionedFiles
#
# Usage:
#
# python write_date_partitions.py --help
# python write_date_partitions.py --output-file-prefix=${HOME}/pipe_tools_cookbook/output/write_date_partitions/shard
# python write_date_partitions.py --count=100 --output-file-prefix=${HOME}/pipe_tools_cookbook/output/write_date_partitions/shard
# python write_date_partitions.py --output-file-prefix=gs://bucket/pipe_tools_cookbook/output/write_date_partitions/shard


class MyPipelineOptions(StandardOptions, TypeOptions):
    DEFAULT_COUNT=10
    DEFAULT_SHARDS_PER_DAY=3
    DEFAULT_FILE_SUFFIX='.json'

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--count',
                            help='number of messages to generate. default %s' % cls.DEFAULT_COUNT,
                            default=cls.DEFAULT_COUNT
        )
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


# build a test pipeline to generate messages and write to a file
def build_pipeline(options):

    standard_options = options.view_as(StandardOptions)
    if standard_options.runner is None:
        standard_options.runner = StandardOptions.DEFAULT_RUNNER

    pipeline = beam.Pipeline(options=options)

    #generate some messages to write
    generator = MessageGenerator(count=int(options.count))
    messages = pipeline | "GenerateMessages" >> GenerateMessages(generator=generator)
    # date partitioning is based on the timestamp used for windowing
    messages = messages | "AddTimestampedValue" >> beam.ParDo(TimestampedValueDoFn())
    messages | "WriteDatePartitions" >> WriteToDatePartitionedFiles(file_path_prefix=options.output_file_prefix,
                                                                    file_name_suffix=options.output_file_suffix,
                                                                    shards_per_day=options.shards_per_day)

    return pipeline


# parse commandline args, build and run a pipeline
def run(args=None):
    # if no arguments given, display usage
    args = args or ['--help']
    options = MyPipelineOptions(args)
    print("Writing {} messages to {}".format(options.count, options.output_file_prefix))

    pipeline = build_pipeline(options)
    result = pipeline.run()
    result.wait_until_finish()

    return 0 if result.state == 'DONE' else 1

# call run() with the args from the command line
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    result = run(args=sys.argv[1:])
    sys.exit(result)

