import pytest
from datetime import datetime
from datetime import timedelta

from airflow import configuration, DAG
from airflow.utils.state import State
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import initdb

from pipe_tools.airflow.operators.python_operator import ExecutionDateBranchOperator

DEFAULT_DATE = datetime(2018, 1, 1)
INTERVAL = timedelta(hours=24)


@pytest.fixture(scope='module')
def airflow_init_db():
    configuration.load_test_config()
    initdb()


@pytest.fixture(scope='function')
def dag(airflow_init_db):
    return DAG(
        'airflow_test_dag',
        default_args={
            'owner': 'airflow',
            'start_date': DEFAULT_DATE},
        schedule_interval='@daily')


@pytest.mark.filterwarnings('ignore:Skipping unsupported ALTER:UserWarning')
class TestAirflow:

    def test_ExecutionDateBranchOperator(self, dag):
        date_branches = [
            (None, DEFAULT_DATE - INTERVAL, 'before'),
            (DEFAULT_DATE, DEFAULT_DATE, 'during'),
            (DEFAULT_DATE + INTERVAL, None, 'after'),
        ]

        op = ExecutionDateBranchOperator(date_branches=date_branches, task_id='date_branch', dag=dag)

        before = DummyOperator(task_id='before', dag=dag)
        before.set_upstream(op)
        during = DummyOperator(task_id='during', dag=dag)
        during.set_upstream(op)
        after = DummyOperator(task_id='after', dag=dag)
        after.set_upstream(op)

        dr = dag.create_dagrun(
            run_id="manual__",
            start_date=datetime.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        expected = [
            ('date_branch', State.SUCCESS),
            ('before', State.SKIPPED),
            ('during', State.NONE),
            ('after', State.SKIPPED),

        ]
        actual = [(ti.task_id, ti.state) for ti in dr.get_task_instances()]
        assert set(expected) == set(actual)
