# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 3.1.3 - (2020-11-04)

### Added

  * [Data Pipeline/PIPELINE-229](https://globalfishingwatch.atlassian.net/browse/PIPELINE-229): Adds
    * Predicate for apache_beam.testing.util.assert_that with floating tolerance.

## 3.1.2 - (2020-06-11)

### Added

  * [GlobalFishingWatch/gfw-eng-tasks#110](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/110): Adds
    * use the `rsa:4.0` version instead of `rsa:4.1`.

### Changed

  * [GlobalFishingWatch/gfw-eng-tasks#41](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/41): Changes
   * Supports only python 3.7.
   * Convert the code to use only python 3 sytax.

## 3.1.1 - (2019-03-09)

### Changed

  * [GlobalFishingWatch/gfw-eng-tasks#27](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/27): Changes
    pins ujson library to version 1.35.

## 3.1.0 - (2019-12-13)

### Changed

  * [GFW-Tasks#59](https://github.com/GlobalFishingWatch/pipe-tools/pull/59): Changes
    so that pipe-tools works on both Python 2 and Python 3. These include:
    * Absolute import fixes.
    * Replacing print statement with print function.
    * Division => floor division fixes.
    * Dealing with `bytes/str/unicode` issues.

## 3.0.0 - (2019-03-27)


### CHANGED

**BREAKING CHANGE, Only compatible with 3.x pipelines**

  * [#59] https://github.com/GlobalFishingWatch/pipe-tools/pull/59
    * Update to work with Apache Beam 2.11.
    * Partially automate creation of requirements list for setup.

### ADDED

  * [#59] https://github.com/GlobalFishingWatch/pipe-tools/pull/59
    * Beam requirements can now be imported from `pipe_tools.beam.requirements` 


## 2.0.0 - (2019-03-08)

  * [#968](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/968)
    * Removes all the airflow code and moving to the new repo [airflow-gfw](https://github.com/GlobalFishingWatch/airflow-gfw).
    * introducing changes in the release-0.2.x

## 1.0.0 - (2018-11-21)

  * [#50](https://github.com/GlobalFishingWatch/pipe-tools/pull/50)
    * Pin Airflow version to 1.10.1
    * NOTE: This changes where not tested on existing pipelines with 1.9.0

## 0.2.5

  * [#56](https://github.com/GlobalFishingWatch/pipe-tools/issues/56)
    * Option to generate standard SQL in bigquery QuerHelper

## 0.2.4 - (2018-11-15)

  * [51](https://github.com/GlobalFishingWatch/pipe-tools/pull/51)
    * Returns generator object for `daterange` method.
    * Parse the string date to a Airflow format accepted using `AIRFLOW_DATE`.
    * Iterates and creates empty table for needed dates.
    * Logs all the process to verify each step
    * Includes parse_gcs_url method from Airflow version 1.10.0, needed in case the schema reads from GCS. Not contemplated in Airflow 1.9
    * Creates class `BigQueryHelperCursor` wrapper to create_empty_tables from service cursor. Not contemplated in Airflow 1.9
    * Creates `AirflowException` to return an error in case the creation of tables fails.

## 0.2.1 - 

  * [ ](https://github.com/GlobalFishingWatch/pipe-tools/pull/) Added modified version of the original BigQueryCreateEmptyTableOperator that only add the table if it does not exist, avoiding it to fail on that case. 

## 0.2.0 - (2018-09-13)

  * [#47](https://github.com/GlobalFishingWatch/pipe-tools/pull/47)
    * Fixes `DagFactory` config initialization order so that the `extra_config` is taken into account before `default_args` processing.
    * Adds a new `base_config` argument to `DagFactory` initialization that is used as the base configuration. The configuration that's loaded from the airflow variables is merged into this base config, and then the `extra_config` is merged afterwards.

## 0.1.7 - (2018-08-27)

  * [#46](https://github.com/GlobalFishingWatch/pipe-tools/pull/46)
    * Pin version of airflow to 1.9.0

## 0.1.6 - (2018-05-13)

  * [#40](https://github.com/GlobalFishingWatch/pipe-tools/pull/40)
    * Change parallelization in xdaterange from 8 to 4
  * [#44](https://github.com/GlobalFishingWatch/pipe-tools/pull/44)
    * Airflow dag factory supports @yearly schedule interval and exponential backoff on retry

## 0.1.5 - (2018-03-25)

  * [#34](https://github.com/GlobalFishingWatch/pipe-tools/pull/34)
    * Automatically set pool based on the runner in DataFlowDirectRunnerOperator
  * [#36](https://github.com/GlobalFishingWatch/pipe-tools/pull/36)
    * Utility bash scripts - xdaterange
  * [#37](https://github.com/GlobalFishingWatch/pipe-tools/pull/37)
    * DagFactory

## 0.1.4 - (2018-03-11)

  * [#27](https://github.com/GlobalFishingWatch/pipe-tools/pull/27)
    * GCP source and sink classes for generic GCP read/write
  * [#31](https://github.com/GlobalFishingWatch/pipe-tools/pull/31)
    * Common tools for airflow dags

## 0.1.3 - (2018-01-01)

  * ['#23'](https://github.com/GlobalFishingWatch/pipe-tools/pull/23)
    * bugfixes for timestamp handling edgecases

## 0.1.2 - (2017-12-15)

  * ['#16'](https://github.com/GlobalFishingWatch/pipe-tools/pull/16)
    * Update cli options handling an help display to be compatible with PipelineOptions model
    * Standardized uuid generator with GFW namespace

  * ['#18'](https://github.com/GlobalFishingWatch/pipe-tools/pull/18)
    * bug fix for writing partitioned files with 0 rows

## 0.1.1 - (2017-11-24)

  * ['#5'](https://github.com/GlobalFishingWatch/pipe-tools/pull/5)
    * New tool for detecting schema of bigquery tables
  * ['#7'](https://github.com/GlobalFishingWatch/pipe-tools/pull/7)
    * Cookbook exampledemonstrates how to use ValueProviders to deploy a pipeline as a template in Dataflow, and how to launch a templated pipeline with Cloud Functions
  * ['#9'](https://github.com/GlobalFishingWatch/pipe-tools/pull/9)
    * Refactor date partitioned writer to provide more generic key partitioned write for any arbitrary key values


## 0.1.0 - (2017-10-22)

  * Initial release.
