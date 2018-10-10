# Monkey patch TaskInstance to get tests to run -- probably a better solution
from airflow.models import TaskInstance
TaskInstance.max_tries = 16