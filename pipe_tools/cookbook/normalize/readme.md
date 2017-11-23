# Dataflow pipeline to normalize shipname and callsign

For https://github.com/GlobalFishingWatch/pipe-tools/issues/8


## Setup
To run these examples, you will need to set up sone configuration settings 

```console 
cp config-template.sh config.sh
nano config.sh
```
Edit PROJECT and BUCKET_NAME in config.sh to appropriate settings.  


## Execution
 
the `normalize.sh` script controls everything.  Run it with no parameters for a list of commands

```console
./normalize.sh
```

