import re

from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.dataflow_operator import GoogleCloudBucketHelper
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook
from airflow.contrib.hooks.gcp_dataflow_hook import _Dataflow
from airflow.utils.decorators import apply_defaults


class DataFlowDirectRunnerHook(DataFlowHook):
    def _start_dataflow(self, task_id, variables, dataflow, name, command_prefix):
        cmd = command_prefix + self._build_cmd(task_id, variables, dataflow)
        _Dataflow(cmd).wait_for_done()

    def _build_cmd(self, task_id, variables, dataflow):
        command = [dataflow]
        if variables is not None:
            for attr, value in variables.iteritems():
                command.append("--" + attr + "=" + value)
        return command


class DataFlowDirectRunnerOperator(DataFlowPythonOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(DataFlowDirectRunnerOperator, self).__init__(*args, **kwargs)

        self.pool = self.pool or self._default_pool

    @property
    def _is_direct_runner(self):
        runner = self.options.get('runner', self.dataflow_default_options.get('runner'))
        return runner == 'DirectRunner'

    @property
    def _default_pool(self):
        return 'local_high_cpu' if self._is_direct_runner else 'dataflow'

    def execute_direct_runner(self, context):
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        dataflow_options = self.dataflow_default_options.copy()
        dataflow_options.update(self.options)
        # Convert argument names from lowerCamelCase to snake case.
        camel_to_snake = lambda name: re.sub(
            r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)
        formatted_options = {camel_to_snake(key): dataflow_options[key]
                             for key in dataflow_options}
        hook = DataFlowDirectRunnerHook(gcp_conn_id=self.gcp_conn_id,
                                        delegate_to=self.delegate_to)
        hook.start_python_dataflow(
            self.task_id, formatted_options,
            self.py_file, self.py_options)

        pass

    def execute(self, context):
        if self._is_direct_runner:
            return self.execute_direct_runner(context=context)
        else:
            return super(DataFlowDirectRunnerOperator, self).execute(context=context)
