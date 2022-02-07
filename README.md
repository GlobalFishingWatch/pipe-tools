# pipe-tools

Package providing [Apache Beam](https://beam.apache.org/) pipeline tools and utilities.

## Development and Testing

You just need [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) in your machine to run the pipeline. No other dependency is required.

Run the unit tests
```console
docker-compose run test
```

If you change any python dependencies, you will need to re-build with
```console
docker-compose build
```
