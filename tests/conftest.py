import posixpath
import os
import tempfile
import shutil
import sys
import pytest


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true",
                     default=False, help="run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


TESTS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = posixpath.join(TESTS_DIR, 'data')


# NB:  This module is magically imported when you run py.test
# and the fixtures below are magically provided to any test function in any test module
# without needing to import them or declare them
@pytest.fixture(scope='session')
def test_data_dir():
    return TEST_DATA_DIR


# creates a temp dir which is automatically deleted after each test function that uses it
@pytest.fixture(scope='function')
def temp_dir(request):
    d = tempfile.mkdtemp()

    def fin():
        shutil.rmtree(d, ignore_errors=True)

    request.addfinalizer(fin)  # finalizer is called at the end of the scope defined in the decorator
    return d


# AIRFLOW_HOME special handling
#
# In order to test airflow operators, you need to import airflow.configuration.  Pretty much anything
# you import from airflow will end up importing configuration.py
#
# In configuration.py, it reads the environment variable AIRFLW_HOME at the time of import, and if
# there is no airflow configuration at that location or the environment variable is not set, then it
# initializes everything.  This means that the only way to influence where AIRFLOW_HOME goes is to set
# the environment variable BEFORE configuration.py is imported.
#
# In pytest_configure() below we create a temp dir and set AIRFLOW_HOME to point to that.  This method
# gets called before the individual test files (like test_airflow.py) are imported, so this allows us
# to use a temp dir that we can clean up after we're done
#

this = sys.modules[__name__]
this.temp_airflow_home = None

def pytest_configure(config):
    airflow_home = tempfile.mkdtemp()
    os.environ['AIRFLOW_HOME'] = airflow_home
    os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
    this.temp_airflow_home = airflow_home


def pytest_unconfigure(config):
    if this.temp_airflow_home:
        shutil.rmtree(this.temp_airflow_home, ignore_errors=True)


@pytest.fixture(scope='session')
def airflow_home():
    return this.temp_airflow_home