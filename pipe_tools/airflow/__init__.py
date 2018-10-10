# Monkey patch TaskInstance to get tests to run -- probably a better solution
from airflow.models import TaskInstance
# TaskInstance.retries Tests began failing because an underlying package expects TaskInstances to have a
# `max_retries` parameter. I'd guess some sort of versioning issue. We set `TaskInstance.max_tries` to
# 16 to make the underlying package happy. I'm not certain what it's used for and the 16 is arbitrary.
TaskInstance.max_tries = 16