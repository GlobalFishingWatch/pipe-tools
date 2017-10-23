#!/bin/bash

# Run all the cookbook entries


PROJECT=world-fishing-827
BUCKET=paul-scratch

UUID=`uuidgen`
TEMP_DATASET=temp_pipe_tools_${UUID//-/_}

echo "creating temp dataset"
echo "  ${TEMP_DATASET}"
bq mk ${TEMP_DATASET}

echo "Creating local temp dir"
TEMPDIR=`mktemp -d`
echo "  ${TEMPDIR}"

GCS_TEMP=gs://${BUCKET}/temp-${UUID}


python pipeline_options.py --count=100 \
  --output-file-prefix=${TEMPDIR}/pipe_tools_cookbook/output/pipeline_options/shard

python write_date_partitions.py --count=100 \
  --output-file-prefix=${TEMPDIR}/pipe_tools_cookbook/output/write_date_partitions/shard

python read_bigquery.py \
    --runner=DirectRunner \
    --output-file-prefix=${TEMPDIR}/pipe_tools_cookbook/output/read_from_bigquery/shard \
    --project=${PROJECT} \
    --query=@query-8k-rows-2-fields.sql

python write_bq_date_partitions.py \
    --runner=DirectRunner \
    --output-table=${PROJECT}:${TEMP_DATASET}.write_bq_date_partitions \
    --project=${PROJECT} \
    --temp_location=${GCS_TEMP}/dataflow-temp/ \
    --staging_location=g${GCS_TEMP}/dataflow-staging/ \

python read_write_bigquery.py \
    --runner=DirectRunner \
    --output-table=${PROJECT}:${TEMP_DATASET}.read_write_bigquery_ \
    --project=${PROJECT} \
    --temp_location=${GCS_TEMP}/dataflow-temp/ \
    --staging_location=g${GCS_TEMP}/dataflow-staging/ \
    --query=@query-8k-rows-2-fields.sql \
    --schema=mmsi:INTEGER,timestamp:TIMESTAMP

SAMPLE_TABLE=${PROJECT}:${TEMP_DATASET}.8k_rows_2_fields
DEST_TABLE=${PROJECT}:${TEMP_DATASET}.schema_autodetect_dest
bq query --max_rows=0 --destination_table=${SAMPLE_TABLE}  < query-8k-rows-2-fields.sql
python schema_autodetect.py \
    --runner=DirectRunner \
    --source-table=${SAMPLE_TABLE} \
    --dest-table=${DEST_TABLE} \
    --project=${PROJECT}
bq query "select count(*) as record_count from [${DEST_TABLE}] LIMIT 100"

echo "Cleaning up temp gcs storage"
echo "  ${GCS_TEMP}"
gsutil -m rm -r ${GCS_TEMP}
echo "Cleaning up temp dataset"
echo "  ${TEMP_DATASET}"
bq rm -rf  ${TEMP_DATASET}
echo "Cleaning up temp dir"
echo "  ${TEMPDIR}"
rm -rf TEMPDIR
