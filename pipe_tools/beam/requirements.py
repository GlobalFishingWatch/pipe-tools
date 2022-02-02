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


BEAM_VERSION = "2.35.0"

BEAM_PACKAGE = ["apache-beam==" + BEAM_VERSION]

REQUIRED_PACKAGES = [
    # Avro 1.9.2 for python3 was broken. The issue was fixed in version 1.9.2.1
    'avro-python3>=1.8.1,!=1.9.2,<1.10.0',
    'crcmod>=1.7,<2.0',
    # Dill doesn't have forwards-compatibility guarantees within minor version.
    # Pickles created with a new version of dill may not unpickle using older
    # version of dill. It is best to use the same version of dill on client and
    # server, therefore list of allowed versions is very narrow.
    # See: https://github.com/uqfoundation/dill/issues/341.
    'dill>=0.3.1.1,<0.3.2',
    'fastavro>=0.21.4,<2',
    'future>=0.18.2,<1.0.0',
    'grpcio>=1.29.0,<2',
    'hdfs>=2.1.0,<3.0.0',
    'httplib2>=0.8,<0.18.0',
    'mock>=1.0.1,<3.0.0',
    # TODO(BEAM-11731): Support numpy 1.20.0
    'numpy>=1.14.3,<1.20.0',
    'pymongo>=3.8.0,<4.0.0',
    'oauth2client>=2.0.1,<5',
    'protobuf>=3.12.2,<4',
    'pyarrow>=0.15.1,<3.0.0',
    'pydot>=1.2.0,<2',
    'python-dateutil>=2.8.0,<3',
    'pytz>=2018.3',
    'requests>=2.24.0,<3.0.0',
    'typing-extensions>=3.7.0,<3.8.0',
    ]

# [BEAM-8181] pyarrow cannot be installed on 32-bit Windows platforms.
if sys.platform == 'win32' and sys.maxsize <= 2**32:
  REQUIRED_PACKAGES = [
      p for p in REQUIRED_PACKAGES if not p.startswith('pyarrow')
  ]

REQUIRED_TEST_PACKAGES = [
    'apache-beam[test]=='+BEAM_VERSION,
]

GCP_REQUIREMENTS = [
    'apache-beam[gcp]=='+BEAM_VERSION,
]

INTERACTIVE_BEAM = [
    'facets-overview>=1.0.0,<2',
    'ipython>=5.8.0,<8',
    'ipykernel>=5.2.0,<6',
    'jupyter-client>=6.1.11,<7',
    'timeloop>=1.0.2,<2',
]

INTERACTIVE_BEAM_TEST = [
    # notebok utils
    'nbformat>=5.0.5,<6',
    'nbconvert>=5.6.1,<6',
    # headless chrome based integration tests
    'selenium>=3.141.0,<4',
    'needle>=0.5.0,<1',
    'chromedriver-binary>=87,<88',
    # use a fixed major version of PIL for different python versions
    'pillow>=7.1.1,<8',
]

AWS_REQUIREMENTS = [
    'apache-beam[aws]=='+BEAM_VERSION
]

AZURE_REQUIREMENTS = [
    'apache-beam[azure]=='+BEAM_VERSION
]

# DATAFLOW_PINNED_DEPENDENCIES = BEAM_PACKAGE + REQUIRED_PACKAGES + GCP_REQUIREMENTS
DATAFLOW_PINNED_DEPENDENCIES = BEAM_PACKAGE + GCP_REQUIREMENTS
requirements = DATAFLOW_PINNED_DEPENDENCIES
