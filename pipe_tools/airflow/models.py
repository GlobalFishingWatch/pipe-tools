from datetime import timedelta

from pipe_tools.airflow import config as config_tools
from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor


class DagFactory(object):
    def __init__(self, pipeline, schedule_interval='@daily', extra_default_args=None, extra_config=None):
        self.pipeline = pipeline
        self.config = config_tools.load_config(pipeline)
        self.default_args = config_tools.default_args(self.config)

        self.default_args.update(extra_default_args or {})
        self.config.update(extra_config or {})

        self.schedule_interval = schedule_interval

    def source_sensor_date_nodash(self):
        if self.schedule_interval == '@daily':
            return '{{ ds_nodash }}'
        elif self.schedule_interval == '@monthly':
            return '{last_day_of_month_nodash}'.format(**self.config)
        elif self.schedule_interval == '@yearly':
            return '{last_day_of_year_nodash}'.format(**self.config)
        else:
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def source_date_range(self):
        if self.schedule_interval == '@daily':
            return '{{ ds }}', '{{ ds }}'
        elif self.schedule_interval == '@monthly':
            return self.config['first_day_of_month'], self.config['last_day_of_month']
        elif self.schedule_interval == '@yearly':
            return self.config['first_day_of_year'], self.config['last_day_of_year']
        else:
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def format_date_sharded_table(self, table, date=None):
        return "{}{}".format(table, date or '')

    def format_bigquery_table(self, project, dataset, table, date=None):
        return "{}:{}.{}".format(project, dataset, self.format_date_sharded_table(table, date))

    def source_tables(self):
        tables = self.config.get('source_tables') or self.config.get('source_table')
        assert tables
        return tables.split(',')

    def source_table_parts(self, date=None):
        tables = self.source_tables()

        for table in tables:
            yield dict(
                project=self.config['project_id'],
                dataset=self.config['source_dataset'],
                table=table,
                date=date
            )

    def source_table_paths(self, date=None):
        return [self.format_bigquery_table(**parts) for parts in self.source_table_parts(date=date)]

    def source_table_date_paths(self):
        return [self.format_bigquery_table(**parts)
                for parts in self.source_table_parts(date=self.source_sensor_date_nodash())]

    def table_sensor(self, dag, task_id, project, dataset, table, date=None):
        return BigQueryTableSensor(
            dag=dag,
            task_id=task_id,
            project_id=project,
            dataset_id=dataset,
            table_id=self.format_date_sharded_table(table, date),
            poke_interval=10,  # check every 10 seconds for a minute
            timeout=60,
            retries=24 * 7,  # retry once per hour for a week
            retry_delay=timedelta(minutes=60)
        )

    def source_table_sensors(self, dag):
        return [
            self.table_sensor(dag=dag, task_id='source_exists_{}'.format(parts['table']), **parts)
            for parts in self.source_table_parts(date=self.source_sensor_date_nodash())
        ]

    def build(self, dag_id):
        raise NotImplementedError
