import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from apache_beam.coders import typecoders

from pipe_tools.generator import GenerateMessages
from pipe_tools.io.datepartitionedsink import WriteToDatePartitionedFiles
from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.timestamp import ParseBeamBQStrTimestampDoFn
from pipe_tools.coders import ReadAsJSONDict
from pipe_tools.coders import JSONDict

def pipeline_options():
    # defne pipeline options

    options = PipelineOptions(setup_file='./setup.py')

    return options


def build(pipeline_options):
    # build a test pipeline

    # 7.5 B messages
    query_params_7B=dict(
        table='world-fishing-827:pipeline_measures_p_p516_daily.',
        start_date='2016-01-01',
        end_date='2017-01-01',
        filter='True'
    )

    # 229M messages
    query_params_229M=dict(
        table='world-fishing-827:pipeline_measures_p_p516_daily.',
        start_date='2017-01-01',
        end_date='2017-02-01',
        filter = 'mmsi >= 200000000 and mmsi < 300000000',
        # filter='mmsi >= 538000000 and mmsi < 538100000'
        # filter='mmsi >= 538000000 and mmsi < 538002000'
        # filter = 'mmsi in (538001663)'
    )

    # small messages
    query_params_small=dict(
        table='world-fishing-827:pipeline_measures_p_p516_daily.',
        start_date='2017-01-01',
        end_date='2017-02-01',
        filter='mmsi in (538001663)'
    )
    fields_2= 'mmsi, timestamp'
    fields_17 = """type,  mmsi,  imo,  shipname,  shiptype,  shiptype_text,  callsign,  timestamp,
                    lon,  lat,  speed,  course,  heading,  distance_from_shore,  distance_from_port,
                    tagblock_station,  gridcode """

    query_template="""select {fields}
        from TABLE_DATE_RANGE([{table}], TIMESTAMP('{start_date}'), TIMESTAMP('{end_date}'))
        where {filter}
    """

    query_params = query_params_229M
    query_params['fields'] = fields_17
    shards_per_day = 16
    query=query_template.format(**query_params)
    # bq_table='world-fishing-827:scratch_paul.shard_test_a_20170101'
    file_path_prefix='gs://paul-scratch/test-date-partitioned-files/test-2017-10-19-b/shard'
    # file_path_prefix='./output/shard'
    file_name_suffix='.json'

    pipeline = beam.Pipeline(options=pipeline_options)

    source = beam.io.gcp.bigquery.BigQuerySource(query=query)

    (
        # pipeline | "Generate Messages" >> GenerateMessages()
        pipeline | "ReadFromBigQuery" >> ReadAsJSONDict(source)
        | "ConvertTimestamp" >> beam.ParDo(ParseBeamBQStrTimestampDoFn())
        | "AddTimestampedValue" >> beam.ParDo(TimestampedValueDoFn())
        | "WriteDatePartitions" >> WriteToDatePartitionedFiles(file_path_prefix, file_name_suffix, shards_per_day=shards_per_day)
    )
    return pipeline


def run(args=None, force_wait=False):

    pipeline = build(pipeline_options())
    job = pipeline.run()

    # job.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
