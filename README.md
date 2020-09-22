[![Build Status](https://travis-ci.org/GlobalFishingWatch/pipe-tools.svg?branch=master)](https://travis-ci.org/GlobalFishingWatch/pipe-tools)

# pipe-tools

It is a package providing [Dataflow](https://cloud.google.com/dataflow/) pipeline tools and utilities.

## Usage

The `pipe-tools` module provides the Dataflow tools build for GFW purposes.
It contains the tools to interpret the JSONDict when the Dataflow analyse them.
Defines the options for GFW pipelines. Handle the way how to read and write over BigQuery. Utils in the transformations and tools for handling date formats and generates universal unique identifiers.

You can find the registry of changes in the `CHANGES.md` file.

Every time you want to make a change in this repo, please run the test or generate the proper ones.

Build/rebuild docker image with
  `docker-compose build` 

To Run unit tests use the following
  `docker-compose run test`
