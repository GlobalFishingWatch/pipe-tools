# *DEPRECATED* pipe-tools

Package providing [Apache Beam](https://beam.apache.org/) pipeline tools and utilities useful for some of our older Beam pipelines at Global Fishing Watch. If you are writing a new pipeline, chances are most of these utilities are already supported by the core beam libraries, and you shouldn't be adding this dependency anymore.

## Development

You just need [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) in your machine to run the pipeline. No other dependency is required. This defines a `dev` service you can use to run any command in an autommatic development environment. For example, to run unit tests, just run `docker-compose run dev pytest`.
