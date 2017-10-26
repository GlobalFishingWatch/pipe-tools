# Dataflow pipeline automation example

Demonstrates a basic automation setup for a dataflow pipeline

## Setup
Assuming you have pipe-tools already installed

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

