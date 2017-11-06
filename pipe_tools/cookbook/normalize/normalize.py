"""Copies from one BigQuery table to another and normalizes the shipname and callsign fields"""

from __future__ import absolute_import

import logging
import sys

import apache_beam as beam
from apache_beam.io import BigQuerySource
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.io.gcp.bigquery import BigQueryDisposition

from pipe_tools.utils.timestamp import as_timestamp
from pipe_tools.io.bigquery import QueryHelper
from pipe_tools.coders import ReadAsJSONDict
from pipe_tools.io import WriteToBigQueryDatePartitioned
from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn


from pipe_tools.cookbook.normalize.shipdataprocess import  normalize_shipname
from pipe_tools.cookbook.normalize.shipdataprocess import normalize_callsign
from stdnum import imo as imo_validator


NORMALIZED_SHIPNAME='normalized_shipname'
NORMALIZED_CALLSIGN='normalized_callsign'
VALID_IMO='valid_imo'


class NormalizeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments
        parser.add_argument(
            '--source_table',
            required=True,
            help='Fully qualified name prefix of the date-sharded table to read from. '
                 'eg PROJECT:DATASET.TABLE_ will be expanded to PROJECT:DATASET.TABLE_YYYYMMDD')
        parser.add_argument(
            '--dest_table',
            required=True,
            help='Fully qualified name prefix of the date-sharded table to write to.')
        parser.add_argument(
            '--date_range',
            required=True,
            help='Range of dates to read.  format YYYY-MM-DD,YYYY-MM-DD')
        parser.add_argument(
            '--wait',
            default=False,
            action='store_true',
            help='Wait until the job finishes before returning')
        parser.add_argument(
            '--mmsi_quotient',
            type=int,
            default=1,
            help='describes the proportion of mmsi values to include from the source table.'
                 'Used for testing to select a small slice of the souce data.  Set to 100 to '
                 'select 1 out of 100 mmsi values.  Set to 1 to get all mmsi (the default)')


class NormalizeNamesDoFn(beam.DoFn):

  def process(self, element):
    element[NORMALIZED_SHIPNAME] = normalize_shipname(element['shipname'])
    element[NORMALIZED_CALLSIGN] = normalize_callsign(element['callsign'])
    imo = element['imo']
    if imo_validator.is_valid(str(imo)):
        element[VALID_IMO] = imo
    yield element


def parse_date_range(s):
    # parse a string YYYY-MM-DD,YYYY-MM-DD into 2 timestamps
    return map(as_timestamp, s.split(','))


def run(args=None):
  pipeline_options = PipelineOptions(args)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options.view_as(SetupOptions).save_main_session = True

  normalize_options = pipeline_options.view_as(NormalizeOptions)
  gcp_options = pipeline_options.view_as(GoogleCloudOptions)

  d1, d2 = parse_date_range(normalize_options.date_range)
  helper = QueryHelper(table=normalize_options.source_table, first_date_ts=d1, last_date_ts=d2)
  select_fields = ['mmsi', 'timestamp', 'seg_id', 'shipname', 'callsign', 'imo']
  where_sql = 'shipname is not null or callsign is not null or imo is not null'
  if normalize_options.mmsi_quotient > 1:
      where_sql = "hash(mmsi) % {} = 0 and ({})".format(normalize_options.mmsi_quotient, where_sql)

  source_schema = helper.filter_table_schema(select_fields)
  source = BigQuerySource(query=helper.build_query(include_fields=select_fields, where_sql=where_sql))

  dest_schema = TableSchema(fields=source_schema.fields)
  dest_schema.fields.append(TableFieldSchema(name=NORMALIZED_SHIPNAME, type='STRING'))
  dest_schema.fields.append(TableFieldSchema(name=NORMALIZED_CALLSIGN, type='STRING'))
  dest_schema.fields.append(TableFieldSchema(name=VALID_IMO, type='INTEGER'))

  pipeline = beam.Pipeline(options=pipeline_options)
  (
      pipeline
      | "ReadSource" >> ReadAsJSONDict(source)
      | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn())
      | "AddTimestamp" >> beam.ParDo(TimestampedValueDoFn())
      | "NormalizeNames" >> beam.ParDo(NormalizeNamesDoFn())
      | "WriteDest" >> WriteToBigQueryDatePartitioned(
          temp_gcs_location=gcp_options.temp_location,
          table=normalize_options.dest_table,
          schema=dest_schema,
          write_disposition=BigQueryDisposition.WRITE_TRUNCATE)
  )

  result = pipeline.run()
  success_states = set([PipelineState.DONE])

  if normalize_options.wait:
    result.wait_until_finish()
  else:
      success_states.add(PipelineState.RUNNING)

  return 0 if result.state in success_states else 1

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  result = run(args=sys.argv[1:])
  sys.exit(result)
