# pipe-tools
Dataflow pipeline tools and utilities

## Install
```console 
virtualenv venv
source venv/bin/activate
pip install --upgrade pip
pip install .
```

## Run

This is just a library of tools, so it does not really do anything.  However there is a basic cli 
you can use to test stuff.  Just modify __main__.py

### Running something locally
```console
python -m pipe_tools  --runner=DirectRunner
```

### Running something remotely
```console
python -m pipe_tools  --runner=DataflowRunner \
  --temp_location=gs://paul-scratch/dataflow-temp/ \
  --staging_location=gs://paul-scratch/dataflow-staging/ \ 
  --project=world-fishing-827 
```

## Development and Testing

```cconsole
pip install -e .
py.test tests
```
