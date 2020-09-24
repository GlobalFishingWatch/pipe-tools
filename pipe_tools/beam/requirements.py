"""Requirements for Apache Beam

Module for holding the dependencies for the currently
used Apache Beam version. 

To update the dependencies:

1. Go to https://beam.apache.org/documentation/sdks/python-dependencies/
   and select the version of beam you want to use.

2. Copy the lists from the correct version of setup.py and replace the contents  below.

3. Update `BEAM_VERSION` to version you selected in 1.

4. Update 'requirements' at the end of the file as needed

"""
import sys


BEAM_VERSION = "2.24.0"

BEAM_PACKAGE = ["apache_beam==" + BEAM_VERSION]

REQUIRED_PACKAGES = [
    # Apache Avro does not follow semantic versioning, so we should not auto
    # upgrade on minor versions. Due to AVRO-2429, Dataflow still
    # requires Avro 1.8.x.
    'avro>=1.8.1,<1.10.0; python_version < "3.0"',
    # Avro 1.9.2 for python3 was broken. The issue was fixed in version 1.9.2.1
    'avro-python3>=1.8.1,!=1.9.2,<1.10.0; python_version >= "3.0"',
    'crcmod>=1.7,<2.0',
    # Dill doesn't have forwards-compatibility guarantees within minor version.
    # Pickles created with a new version of dill may not unpickle using older
    # version of dill. It is best to use the same version of dill on client and
    # server, therefore list of allowed versions is very narrow.
    # See: https://github.com/uqfoundation/dill/issues/341.
    'dill>=0.3.1.1,<0.3.2',
    'fastavro>=0.21.4,<0.24',
    'funcsigs>=1.0.2,<2; python_version < "3.0"',
    'future>=0.18.2,<1.0.0',
    'futures>=3.2.0,<4.0.0; python_version < "3.0"',
    'grpcio>=1.29.0,<2',
    'hdfs>=2.1.0,<3.0.0',
    'httplib2>=0.8,<0.18.0',
    'mock>=1.0.1,<3.0.0',
    'numpy>=1.14.3,<2',
    'pymongo>=3.8.0,<4.0.0',
    'oauth2client>=2.0.1,<4',
    'protobuf>=3.12.2,<4',
    # [BEAM-6287] pyarrow is not supported on Windows for Python 2
    ('pyarrow>=0.15.1,<0.18.0; python_version >= "3.0" or '
     'platform_system != "Windows"'),
    'pydot>=1.2.0,<2',
    'python-dateutil>=2.8.0,<3',
    'pytz>=2018.3',
    # [BEAM-5628] Beam VCF IO is not supported in Python 3.
    'pyvcf>=0.6.8,<0.7.0; python_version < "3.0"',
    # fixes and additions have been made since typing 3.5
    'requests>=2.24.0,<3.0.0',
    'typing>=3.7.0,<3.8.0; python_full_version < "3.5.3"',
    'typing-extensions>=3.7.0,<3.8.0',
    ]

# [BEAM-8181] pyarrow cannot be installed on 32-bit Windows platforms.
if sys.platform == 'win32' and sys.maxsize <= 2**32:
    REQUIRED_PACKAGES = [
      p for p in REQUIRED_PACKAGES if not p.startswith('pyarrow')
    ]

REQUIRED_TEST_PACKAGES = [
    'freezegun>=0.3.12',
    'nose>=1.3.7',
    'nose_xunitmp>=0.4.1',
    'pandas>=0.24.2,<1; python_full_version < "3.5.3"',
    'pandas>=0.25.2,<1; python_full_version >= "3.5.3"',
    'parameterized>=0.7.1,<0.8.0',
    # pyhamcrest==1.10.0 doesn't work on Py2. Beam still supports Py2.
    # See: https://github.com/hamcrest/PyHamcrest/issues/131.
    'pyhamcrest>=1.9,!=1.10.0,<2.0.0',
    'pyyaml>=3.12,<6.0.0',
    'requests_mock>=1.7,<2.0',
    'tenacity>=5.0.2,<6.0',
    'pytest>=4.4.0,<5.0',
    'pytest-xdist>=1.29.0,<2',
    'pytest-timeout>=1.3.3,<2',

    # Override to use rsa==4.0 per https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/110
    #'rsa<4.1; python_version < "3.0"',
    'rsa==4.0',

    # sqlalchemy is used only for running xlang jdbc test so limit to Py3
    'sqlalchemy>=1.3,<2.0; python_version >= "3.5"',
    # psycopg is used only for running xlang jdbc test so limit to Py3
    'psycopg2-binary>=2.8.5,<3.0.0; python_version >= "3.5"',
    # testcontainers is used only for running xlang jdbc test so limit to Py3
    'testcontainers>=3.0.3,<4.0.0; python_version >= "3.5"',
    ]

GCP_REQUIREMENTS = [
    'cachetools>=3.1.0,<4',
    'google-apitools>=0.5.31,<0.5.32',
    'google-auth>=1.18.0,<2',
    'google-cloud-datastore>=1.7.1,<2',
    'google-cloud-pubsub>=0.39.0,<2',
    # GCP packages required by tests
    'google-cloud-bigquery>=1.6.0,<2',
    'google-cloud-core>=0.28.1,<2',
    'google-cloud-bigtable>=0.31.1,<2',
    'google-cloud-spanner>=1.13.0,<2',
    'grpcio-gcp>=0.2.2,<1',
    # GCP Packages required by ML functionality
    'google-cloud-dlp>=0.12.0,<2',
    'google-cloud-language>=1.3.0,<2',
    'google-cloud-videointelligence>=1.8.0,<2',
    'google-cloud-vision>=0.38.0,<2',
]

INTERACTIVE_BEAM = [
    'facets-overview>=1.0.0,<2',
    'ipython>=5.8.0,<8',
    'ipykernel>=5.2.0,<6',
    'timeloop>=1.0.2,<2',
]

INTERACTIVE_BEAM_TEST = [
    # notebok utils
    'nbformat>=5.0.5,<6',
    'nbconvert>=5.6.1,<6',
    'jupyter-client>=6.1.2,<7',
    # headless chrome based integration tests
    'selenium>=3.141.0,<4',
    'needle>=0.5.0,<1',
    'chromedriver-binary>=83,<84',
    # use a fixed major version of PIL for different python versions
    'pillow>=7.1.1,<8',
]

AWS_REQUIREMENTS = [
    'boto3 >=1.9'
]

DATAFLOW_PINNED_DEPENDENCIES = BEAM_PACKAGE + REQUIRED_PACKAGES + GCP_REQUIREMENTS
requirements = DATAFLOW_PINNED_DEPENDENCIES
