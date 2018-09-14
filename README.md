[![Build Status](https://travis-ci.org/GlobalFishingWatch/pipe-tools.svg?branch=master)](https://travis-ci.org/GlobalFishingWatch/pipe-tools)

# pipe-tools

[Airflow](https://airflow.apache.org/) / [Dataflow](https://cloud.google.com/dataflow/) pipeline tools and utilities.

## Installation

TODO: Document the installation process

## Usage

TODO: Document the utilities that we provide and how to use them

## Development

To setup your development environment you have 2 options. The recommended way is through [docker](https://www.docker.com/), although you can also use a local [python](https://www.python.org/) installation if you prefer so.

### Docker

If you have [docker](https://www.docker.com/) and [docker compose](https://docs.docker.com/compose/) on your machine, we provide an image which contains everything that's needed. Just run the following command to run the tests:

```console
sudo docker-compose run test
```

### Local python

You need to have [python](https://www.python.org/) 2.7. [Virtualenv](https://virtualenv.pypa.io/en/stable/) is recommended as well. Just run the following commands to run the tests, and you are ready to start hacking around:

```cconsole
virtualenv venv
source venv/bin/activate
pip install -e .
py.test tests
```
