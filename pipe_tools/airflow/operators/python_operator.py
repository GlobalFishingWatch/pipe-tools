from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.decorators import apply_defaults


def branch_on_execution_date(**kwargs):
    print kwargs
    dt = kwargs['execution_date']
    for d1, d2, task in kwargs['branches']:
        if (d1 is None or dt >= d1) and (d2 is None or dt <= d2):
            return task
    raise ValueError('date_branch_operator: No matching date range found %s' % kwargs)


class ExecutionDateBranchOperator(BranchPythonOperator):
    """
    Branch on execution date.   Takes a list of tuples containing a date range and a taskid.
    If the execution date is in the range (inclusive), then the corresponding taskid will be returned.
    The first matching range in the list of branches will be used

    example:

    date_branches = [
        (None, datetime(2016, 12, 31), 'nodata'),
        (datetime(2017, 1, 1), datetime(2017, 6, 30), 'historic'),
        (datetime(2017, 7, 1), None, 'daily'),
    ]

    """
    @apply_defaults
    def __init__(
            self,
            date_branches,
            **kwargs):

        super(ExecutionDateBranchOperator, self).__init__(
            python_callable=branch_on_execution_date,
            op_kwargs=dict(branches=date_branches),
            provide_context=True,
            **kwargs)
