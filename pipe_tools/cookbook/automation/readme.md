# Dataflow pipeline automation example

Demonstrates a basic automation setup for a dataflow pipeline

The Cloud Functions implementation is based in this example:
https://dzone.com/articles/triggering-dataflow-pipelines-with-cloud-functions


## Setup
To run these examples, you will need to set up sone configuration settings and install some 
node.js dependencies.  First 

```console 
cp config-template.sh config.sh
nano config.sh
```
Edit PROJECT and BUCKET_NAME in config.sh to appropriate settings.  

In order to run the Cloud Functions you need to have the googleapis node.js library installed

```console
npm install --save googleapis
```

## Execution
 
the `copyfiles.sh` script controls everything.  Run it with no parameters for a list of commands

```console
./copyfiles.sh
```

To view the Cloud Function logs after you run `./copyfiles.sh triggerfn`, use this

```
gcloud beta functions logs read
```

## Development

For local testing of Cloud Functions, use the emulator

https://github.com/GoogleCloudPlatform/cloud-functions-emulator

Note that you can only test Http triggers this way, so bacground functions that are pubsub or gcs triggered, you 
 will have to make an http-triggered version of it to test
 

