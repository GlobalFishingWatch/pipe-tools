Changelog
=========

0.1.6 - (2018-05-13)
--------------------

* [#40](https://github.com/GlobalFishingWatch/pipe-tools/pull/40)
  Change parallelization in xdaterange from 8 to 4
* [#44](https://github.com/GlobalFishingWatch/pipe-tools/pull/44)
  Airflow dag factory supports @yearly schedule interval and exponential backoff on retry


0.1.5 - (2018-03-25)
--------------------

* [#34](https://github.com/GlobalFishingWatch/pipe-tools/pull/34)
  Automatically set pool based on the runner in DataFlowDirectRunnerOperator
* [#36](https://github.com/GlobalFishingWatch/pipe-tools/pull/36)
  Utility bash scripts - xdaterange
* [#37](https://github.com/GlobalFishingWatch/pipe-tools/pull/37)
  DagFactory
  
  
0.1.4 - (2018-03-11)
--------------------

* [#27](https://github.com/GlobalFishingWatch/pipe-tools/pull/27)
  GCP source and sink classes for generic GCP read/write
* [#31](https://github.com/GlobalFishingWatch/pipe-tools/pull/31)
  Common tools for airflow dags
 
 
0.1.3 - (2018-01-01)
--------------------

* ['#23'](https://github.com/GlobalFishingWatch/pipe-tools/pull/23)
  bugfixes for timestamp handling edgecases

0.1.2 - (2017-12-15)
--------------------

* ['#16'](https://github.com/GlobalFishingWatch/pipe-tools/pull/16)
  Update cli options handling an help display to be compatible with PipelineOptions model
  Standardized uuid generator with GFW namespace

* ['#18'](https://github.com/GlobalFishingWatch/pipe-tools/pull/18)
  bug fix for writing partitioned files with 0 rows
  
0.1.1 - (2017-11-24)
--------------------

* ['#5'](https://github.com/GlobalFishingWatch/pipe-tools/pull/5)
  New tool for detecting schema of bigquery tables
* ['#7'](https://github.com/GlobalFishingWatch/pipe-tools/pull/7)
  Cookbook exampledemonstrates how to use ValueProviders to deploy a 
  pipeline as a template in Dataflow, and how to launch a templated 
  pipeline with Cloud Functions
* ['#9'](https://github.com/GlobalFishingWatch/pipe-tools/pull/9)
  Refactor date partitioned writer to provide more generic
  key partitioned write for any arbitrary key values
  
  
0.1.0 - (2017-10-22)
--------------------

* Initial release.
