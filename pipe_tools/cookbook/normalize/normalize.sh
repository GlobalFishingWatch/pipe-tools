#!/usr/bin/env bash

# config.sh should be created by copying config-template.sh and editing the contents
source config.sh

INPUT_TABLE=world-fishing-827:scratch_name_normalize.classify_p516_
OUTPUT_TABLE=world-fishing-827:scratch_name_normalize.normalize_name_test_
DATE_RANGE=2016-01-01,2016-12-31
TEMP_DIR=gs://${BUCKET_NAME}/dataflow-temp
STAGING_DIR=gs://${BUCKET_NAME}/dataflow-staging
JOB_NAME=normalize-names-$(date "+%Y%m%d-%H%M%S")


display_usage() {
	echo -e "\nUsage:\n$0 COMMAND\n"
	echo "Available Commands"
	echo "  local           Run the pipeline locally"
	echo "  remote          Run the pipeline in dataflow"
	}


if [[ $# -le 0 || $# -gt 1 ]]
then
    display_usage
    exit 1
fi


case $1 in

  local)
    echo 'Running local...'
    pip install -r ./requirements.txt

    python -m normalize --source_table=${INPUT_TABLE} \
        --dest_table=${OUTPUT_TABLE} \
        --temp_location ${TEMP_DIR} \
        --date_range=2017-01-01,2017-01-03 \
        --project ${PROJECT} \
        --mmsi_quotient 10000 \
        --wait
    echo 'Done.'
    ;;

  remote)
    echo 'Running in dataflow...'
    pip install -r ./requirements.txt
    python -m normalize --source_table=${INPUT_TABLE} \
        --dest_table=${OUTPUT_TABLE} \
        --temp_location ${TEMP_DIR} \
        --date_range=${DATE_RANGE} \
        --project ${PROJECT} \
        --runner=DataflowRunner \
        --staging_location ${STAGING_DIR} \
        --job_name ${JOB_NAME} \
        --max_num_workers 400 \
        --disk_size_gb 50 \
        --setup_file=../../../setup.py \
        --requirements_file=./requirements.txt
    echo 'Done.'
    ;;

  *)
    display_usage
    exit 0
    ;;
esac

