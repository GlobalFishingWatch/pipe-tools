from datetime import datetime
from datetime import timedelta
import math

from airflow.models import Variable


CONNECTION_ID = 'google_cloud_default'


def load_config (variable_name):
    config = Variable.get(variable_name, deserialize_json=True)
    config['ds'] = "{{ ds }}"
    config['ds_nodash'] = "{{ ds_nodash }}"
    config['first_day_of_month'] = '{{ execution_date.replace(day=1).strftime("%Y-%m-%d") }}'
    config['last_day_of_month'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}'
    config['first_day_of_month_nodash'] = '{{ execution_date.replace(day=1).strftime("%Y%m%d") }}'
    config['last_day_of_month_nodash'] = '{{ (execution_date.replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y%m%d") }}'
    config['first_day_of_year'] = '{{ execution_date.replace(day=1, month=1).strftime("%Y-%m-%d") }}'
    config['last_day_of_year'] = '{{ (execution_date.replace(day=1, month=1) + macros.dateutil.relativedelta.relativedelta(years=1, days=-1)).strftime("%Y-%m-%d") }}'
    config['first_day_of_year_nodash'] = '{{ execution_date.replace(day=1, month=1).strftime("%Y%m%d") }}'
    config['last_day_of_year_nodash'] = '{{ (execution_date.replace(day=1, month=1) + macros.dateutil.relativedelta.relativedelta(years=1, days=-1)).strftime("%Y%m%d") }}'
    return config


def pipeline_start_date(config):
    date_str = config.get('pipeline_start_date', Variable.get('PIPELINE_START_DATE', ''))
    if date_str:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    else:
        return datetime.utcnow() - timedelta(days=3)

def pipeline_end_date(config):
    date_str = config.get('pipeline_end_date', Variable.get('PIPELINE_END_DATE', ''))
    if date_str:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    else:
        return None


INITIAL_RETRY_DELAY = 2 * 60


def default_args(config):
    args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': pipeline_start_date(config),
        'end_date': pipeline_end_date(config),
        'email': ['airflow@globalfishingwatch.org'],
        'email_on_failure': False,
        'email_on_retry': False,

        # retry with binary exponential backoff for 24 - 48 hours
        'retry_exponential_backoff': True,
        'retry_delay': timedelta(seconds=INITIAL_RETRY_DELAY),
        'retries': int(math.log(24 * 60 * 60 / INITIAL_RETRY_DELAY, 2)) + 1,

        'project_id': config['project_id'],
        'dataset_id': config['pipeline_dataset'],
        'bucket': config['pipeline_bucket'],
        'bigquery_conn_id': CONNECTION_ID,
        'gcp_conn_id': CONNECTION_ID,
        'google_cloud_conn_id': CONNECTION_ID,
        'write_disposition': 'WRITE_TRUNCATE',
        'allow_large_results': True,
    }

    return args

