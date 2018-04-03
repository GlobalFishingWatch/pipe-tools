import pytest
from datetime import datetime
from datetime import timedelta
import os

from airflow import configuration, DAG
from airflow.utils.state import State
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import initdb
from airflow.models import Variable
from pipe_tools.airflow.operators.python_operator import ExecutionDateBranchOperator
from pipe_tools.airflow.dataflow_operator import DataFlowDirectRunnerOperator
from pipe_tools.airflow.models import DagFactory

DEFAULT_DATE = datetime(2018, 1, 1)
INTERVAL = timedelta(hours=24)


# NB:  See the commennts on conftest.py about how AIRFLOW_HOME gets initialized
@pytest.fixture(scope='module')
def airflow_init_db(airflow_home):
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


@pytest.fixture(scope='function')
def dag_config(airflow_init_db):
    variable_name='pipe_test'
    value = dict(
        project_id='test_project',
        pipeline_dataset='dataset',
        pipeline_bucket='bucket',
        foo='bar',
    )
    Variable.set(variable_name, value, serialize_json=True)
    return variable_name


@pytest.fixture(scope='function')
def dag_factory(airflow_init_db):
    class _Test_DagFactory(DagFactory):
        def build(self, dag_id):
            with DAG('airflow_test_dag', default_args=self.default_args, schedule_interval=self.schedule_interval) as dag:
                op = DummyOperator(task_id='dummy')
                dag >> op

    return _Test_DagFactory


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

    @pytest.mark.parametrize("options,expected", [
        (None, 'dataflow'),
        ({}, 'dataflow'),
        ({'runner':'DataflowRunner'}, 'dataflow'),
        ({'runner': 'DirectRunner'}, 'local-cpu'),
    ])
    def test_DataFlowDirectRunnerOperator_pool(self, options, expected, dag):
        op = DataFlowDirectRunnerOperator(
            task_id='dataflow_direct',
            options=options,
            py_file='dummy.py',
            dag=dag
        )
        assert op.pool == expected

    def test_DagFactory(self, dag_factory, dag_config):
        dag = dag_factory(pipeline=dag_config).build('test_dag')
