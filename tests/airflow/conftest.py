import pytest
import os
import tempfile
import shutil
import sys

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