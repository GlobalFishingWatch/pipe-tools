#!/usr/bin/env bash

# config.sh should be created by copying config-template.sh and editing the contents
source config.sh

WORK_DIR=gs://${BUCKET_NAME}/pipe-tools-cookbook/automation
TEMP_DIR=gs://${BUCKET_NAME}/dataflow-temp
STAGING_DIR=gs://${BUCKET_NAME}/dataflow-staging
TEMPLATE_DIR=${WORK_DIR}/templates
SAMPLE_DATA=sample.txt
JOB_NAME=copyfile-$(date "+%Y%m%d-%H%M%S")
REGION=us-central1


display_usage() {
	echo -e "\nUsage:\n$0 COMMAND\n"
	echo "Available Commands"
	echo "  reset           Delete all GCS output files "
	echo "  generate        Generate a sample data file"
	echo "  local           Run the pipline locally"
	echo "  remote          Run the pipeline in dataflow"
	echo "  maketemplate    Publish the pipeline as a dataflow template"
	echo "  runtemplate     Execute the templated pipeline in dataflow"
	echo "  deployfn        Deploy a cloud function that runs the pipeline in datflow"
	echo "  triggerfn       Trigger the cloud function"
	}


if [[ $# -le 0 || $# -gt 1 ]]
then
    display_usage
    exit 1
fi


case $1 in
  reset)
    gsutil -m rm -rf ${WORK_DIR}
    ;;

  generate)
    echo "0" > ${SAMPLE_DATA}
    for i in `seq 1 99`;
    do
      echo $i >> ${SAMPLE_DATA}
    done
    ;;

  local)
    echo 'Running local...'
    INPUT=${WORK_DIR}/local/sample-in.txt
    OUTPUT=${WORK_DIR}/local/sample-out
    gsutil cp ${SAMPLE_DATA} ${INPUT}
    python -m copyfile --input=${INPUT} --output=${OUTPUT} --code=local
    echo 'Done.'
    gsutil ls ${OUTPUT}*
    gsutil cat ${OUTPUT}*
    ;;

  remote)
    echo 'Running in dataflow...'
    INPUT=${WORK_DIR}/remote/sample-in.txt
    OUTPUT=${WORK_DIR}/remote/sample-out
    gsutil cp ${SAMPLE_DATA} ${INPUT}
    python -m copyfile --input=${INPUT} --output=${OUTPUT} --code=remote \
      --runner=DataflowRunner \
      --project ${PROJECT} \
      --staging_location ${STAGING_DIR} \
      --temp_location ${TEMP_DIR} \
      --job_name ${JOB_NAME}-remote \
      --region ${REGION}

    echo 'Done.'
    gsutil ls ${OUTPUT}*
    gsutil cat ${OUTPUT}*
    ;;

  maketemplate)
    echo 'publishing job as a template'
    INPUT=${WORK_DIR}/maketemplate/sample-in.txt
    OUTPUT=${WORK_DIR}/maketemplate/sample-out
    gsutil cp ${SAMPLE_DATA} ${INPUT}
    python -m copyfile  \
      --runner=DataflowRunner \
      --project ${PROJECT} \
      --staging_location ${STAGING_DIR} \
      --temp_location ${TEMP_DIR}  \
      --template_location ${TEMPLATE_DIR}/copyfile
    echo 'Done.'
    ;;

  runtemplate)
    echo 'running copyfile template in dataflow'

    INPUT=${WORK_DIR}/runtemplate/sample-in.txt
    OUTPUT=${WORK_DIR}/runtemplate/sample-out
    gsutil cp ${SAMPLE_DATA} ${INPUT}

    gcloud beta dataflow jobs run ${JOB_NAME}-runtemplate \
        --gcs-location ${TEMPLATE_DIR}/copyfile \
        --parameters input=${INPUT},output=${OUTPUT},code=runtemplate \
        --region ${REGION}
    ;;

  deployfn)
    gcloud beta functions deploy copyFiles --stage-bucket ${BUCKET_NAME} --trigger-http
    ;;

  triggerfn)
    gcloud beta functions call copyFiles
    ;;

  *)
    display_usage
    exit 0
    ;;
esac


