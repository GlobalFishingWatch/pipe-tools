"""Requirements for Apache Beam

Module for holding the dependencies for the currently
used Apache Beam version. 

To update the dependencies:

1. Go to https://beam.apache.org/documentation/sdks/python-dependencies/
   and select the version of bean you want to use.

2. Copy the table and replace the contents of `beam_requirement_text` below
   with it.

3. Update `beam_version` to version you selected in 1.


"""

beam_version = "2.16.0"

beam_requirement_text = """
avro-python3    >=1.8.1,<2.0.0; python_version >= "3.0"
avro    >=1.8.1,<2.0.0; python_version < "3.0"
cachetools  >=3.1.0,<4
crcmod  >=1.7,<2.0
dill    >=0.3.0,<0.3.1
fastavro    >=0.21.4,<0.22
funcsigs    >=1.0.2,<2; python_version < "3.0"
future  >=0.16.0,<1.0.0
futures >=3.2.0,<4.0.0; python_version < "3.0"
google-apitools >=0.5.28,<0.5.29
google-cloud-bigquery   >=1.6.0,<1.18.0
google-cloud-bigtable   >=0.31.1,<1.1.0
google-cloud-core   >=0.28.1,<2
google-cloud-datastore  >=1.7.1,<1.8.0
google-cloud-pubsub >=0.39.0,<1.1.0
googledatastore >=7.0.1,<7.1; python_version < "3.0"
grpcio  >=1.12.1,<2
hdfs    >=2.1.0,<3.0.0
httplib2    >=0.8,<=0.12.0
mock    >=1.0.1,<3.0.0
oauth2client    >=2.0.1,<4
proto-google-cloud-datastore-v1 >=0.90.0,<=0.90.4; python_version < "3.0"
protobuf    >=3.5.0.post1,<4
pyarrow >=0.11.1,<0.15.0; python_version >= "3.0" or platform_system != "Windows"
pydot   >=1.2.0,<2
pymongo >=3.8.0,<4.0.0
python-dateutil >=2.8.0,<3
pytz    >=2018.3
pyvcf   >=0.6.8,<0.7.0; python_version < "3.0"
pyyaml  >=3.12,<4.0.0
typing  >=3.6.0,<3.7.0; python_version < "3.5.0"
"""

requirements = ["apache_beam==" + beam_version] + beam_requirement_text.split('\n')
