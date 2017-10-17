import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

from pipe_tools.generator import TimestampMessageGenerator
from pipe_tools.io.datepartitionedsink import WriteToDatePartitionedFiles
from pipe_tools.timestamp import TimestampedValueDoFn

def pipeline_options():
    # defne pipeline options

    options = PipelineOptions(setup_file = './setup.py')

    return options


def build(pipeline_options):
    # build a test pipeline

    file_path_prefix='gs://paul-scratch/test-date-partitioned-files/test-2017-10-17-c/shard'
    file_name_suffix='.json'

    pipeline = beam.Pipeline(options=pipeline_options)

    messages = TimestampMessageGenerator()

    (
        pipeline | "Generate Messages" >> beam.Create(messages)
        | "Add Timestamp" >> beam.ParDo(TimestampedValueDoFn())
        | "Write to Date Partitions" >> WriteToDatePartitionedFiles(file_path_prefix, file_name_suffix)
    )
    return pipeline


def run(args=None, force_wait=False):

    pipeline = build(pipeline_options())
    job = pipeline.run()

    job.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
