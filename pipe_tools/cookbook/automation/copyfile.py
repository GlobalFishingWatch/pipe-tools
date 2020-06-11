"""Copies from one file to another and appends a code to every line."""

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class AppendCodeDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def __init__(self, code):
    self.code = code
    super(AppendCodeDoFn, self).__init__()

  def process(self, element):
    yield "{element}.{code}".format(element=element, code=self.code.get())


def run(argv=None):

  class CopyfileOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
          # Use add_value_provider_argument for arguments to be templatable
          # Use add_argument as usual for non-templatable arguments
          parser.add_value_provider_argument(
              '--input',
              help='Path of the file to read from')
          parser.add_value_provider_argument(
              '--output',
              help='Output file to write results to.')
          parser.add_value_provider_argument(
              '--code',
              help='code to append to the output')

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  copyfile_options = pipeline_options.view_as(CopyfileOptions)
  # Read the text file[pattern] into a PCollection.
  lines = p | 'ReadLines' >> ReadFromText(copyfile_options.input)

  # append the code
  lines = lines | 'AppendCode' >> beam.ParDo(AppendCodeDoFn(copyfile_options.code))

  output = lines | 'WriteLines' >> WriteToText(copyfile_options.output)

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
