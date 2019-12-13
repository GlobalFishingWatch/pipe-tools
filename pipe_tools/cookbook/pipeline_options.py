from __future__ import print_function
import logging
import warnings
import sys
from pipe_tools.beam import logging_monkeypatch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, TypeOptions
from apache_beam.io import WriteToText
from pipe_tools.generator import GenerateMessages
from pipe_tools.generator import MessageGenerator


# Custom Pipeline Options
#
# Demonstrates how to define custom command line arguments for a pipeline
# that integrate with the built in command line argument processing
#
# based on https://beam.apache.org/documentation/programming-guide/#creating-custom-options
#
# Usage:
#
# python pipeline_options.py --help
# python pipeline_options.py --output-file-prefix=${HOME}/pipe_tools_cookbook/output/pipeline_options/shard
# python pipeline_options.py --count=100 --output-file-prefix=${HOME}/pipe_tools_cookbook/output/pipeline_options/shard
# python pipeline_options.py $(cat pipeline_options_args.txt)
#


class MyPipelineOptions(StandardOptions, TypeOptions):
    DEFAULT_COUNT=10

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


# build a test pipeline to generate messages and write to a file
def build_pipeline(options):

    standard_options = options.view_as(StandardOptions)
    if standard_options.runner is None:
        standard_options.runner = StandardOptions.DEFAULT_RUNNER

    pipeline = beam.Pipeline(options=options)
    generator=MessageGenerator(count=int(options.count))
    messages = pipeline | "GenerateMessages" >> GenerateMessages(generator=generator)

    with warnings.catch_warnings():
        # suppress a spurious warning generated within WriteToText.  This warning is annoying but harmless
        warnings.filterwarnings(action="ignore", message="Using fallback coder for typehint: <type 'NoneType'>")

        messages | 'write' >> WriteToText(file_path_prefix=options.output_file_prefix)

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
    logging.getLogger().setLevel(logging.DEBUG)
    result = run(args=sys.argv[1:])
    sys.exit(result)

