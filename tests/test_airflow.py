import pytest
from datetime import datetime
from datetime import timedelta
import os

from airflow import configuration, DAG
from airflow.utils.state import State
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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
    variable_name = 'pipe_test'
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
            return dag

    return _Test_DagFactory


@pytest.mark.filterwarnings('ignore:Skipping unsupported ALTER:UserWarning')
class TestAirflow:

    @staticmethod
    def assert_expected_task(task_id, expected, templated, dag):
        def assert_expected(**kwargs):
            expected = kwargs['templates_dict']['expected']
            actual = kwargs['templates_dict']['actual']
            assert expected == actual

        return PythonOperator(
            task_id=task_id,
            provide_context=True,
            python_callable=assert_expected,
            templates_dict={'expected': expected, 'actual': templated},
            dag=dag)

    def test_ExecutionDateBranchOperator(self, dag):
        date_branches = [
            (None, DEFAULT_DATE - INTERVAL, 'before'),
            (DEFAULT_DATE, DEFAULT_DATE, 'during'),
            (DEFAULT_DATE + INTERVAL, None, 'after'),
        ]

        op = ExecutionDateBranchOperator(
            date_branches=date_branches, task_id='date_branch', dag=dag)

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
        ({'runner': 'DataflowRunner'}, 'dataflow'),
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

    @pytest.mark.parametrize("schedule_interval,expected", [
        ('@daily', '20180101'),
        ('@monthly', '20180131'),
        ('@yearly', '20181231'),
    ])
    def test_schedule_interval_dates(self, schedule_interval, expected, dag_factory, dag_config):
        factory = dag_factory(pipeline=dag_config,
                              schedule_interval=schedule_interval)
        dag = factory.build('interval_test_dag')
        task = self.assert_expected_task(
            task_id='%s%s' % (schedule_interval.replace('@', ''), expected),
            expected=expected,
            templated=factory.source_sensor_date_nodash(),
            dag=dag
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @pytest.mark.parametrize("key,expected", [
        # Generated config entries
        ('ds', '2018-01-01'),
        ('ds_nodash', '20180101'),
        ('first_day_of_month', '2018-01-01'),
        ('last_day_of_month', '2018-01-31'),
        ('first_day_of_month_nodash', '20180101'),
        ('last_day_of_month_nodash', '20180131'),
        ('first_day_of_year', '2018-01-01'),
        ('last_day_of_year', '2018-12-31'),
        ('first_day_of_year_nodash', '20180101'),
        ('last_day_of_year_nodash', '20181231'),

        # Config entries stored in the database
        ('project_id', 'test_project'),
        ('pipeline_dataset', 'dataset'),
        ('pipeline_bucket', 'bucket'),

        # Entries added by extra config
        ('foo', 'baz'),
        ('additional', 'additional_value'),

        # Entries due to the base config
        ('base', 'base_value'),
    ])
    def test_config(self, key, expected, dag_factory, dag_config):
        base_config = {
            'base': 'base_value',
            'project_id': 'other_project',
        }
        extra_config = {
            'foo': 'baz',
            'additional': 'additional_value',
        }
        factory = dag_factory(pipeline=dag_config, base_config=base_config, extra_config=extra_config)
        dag = factory.build('config_test_dag')
        task = self.assert_expected_task(
            task_id=key,
            expected=expected,
            templated=factory.config[key],
            dag=dag
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

