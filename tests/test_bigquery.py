import pytest
import mock
from datetime import datetime
import pytz

from pipe_tools.io.bigquery import encode_table_ref
from pipe_tools.io.bigquery import decode_table_ref
from pipe_tools.io.bigquery import QueryHelper
from pipe_tools.utils.timestamp import as_timestamp

@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestBigquery:

    @pytest.mark.parametrize("table", [
        "Project:dataset.table",
        "dataset.table"
    ])
    def test_tableref_encode(self, table):
        assert encode_table_ref(decode_table_ref(table)) == table

    @pytest.mark.parametrize("value,expected", [
        ('2019-01-01', 'TIMESTAMP(2019-01-01)'),
        (datetime(2019,1,1, tzinfo=pytz.UTC), 'SEC_TO_TIMESTAMP(1546300800)'),
        (1546300800, 'SEC_TO_TIMESTAMP(1546300800)'),
    ])
    def test_query_helper_sql_timestamp(self, value, expected):
        assert QueryHelper._date_to_sql_timestamp(value) == expected


    @pytest.mark.parametrize("is_date_sharded,use_legacy_sql,expected", [
        (False, False,
         "SELECT * FROM `{table}` WHERE True"),

        (False, True,
         "SELECT * FROM [{table}] WHERE True"),

        (True, False,
         ("SELECT * FROM `{table}*` "
         "WHERE _TABLE_SUFFIX BETWEEN "
         "FORMAT_TIMESTAMP('%Y%m%d', SEC_TO_TIMESTAMP({first_date_ts})) "
         "AND FORMAT_TIMESTAMP('%Y%m%d', SEC_TO_TIMESTAMP({last_date_ts})) "
         "AND True") ),

        (True, True,
         ("SELECT * FROM TABLE_DATE_RANGE([{table}]"
          ", SEC_TO_TIMESTAMP({first_date_ts}), SEC_TO_TIMESTAMP({last_date_ts})) "
          "WHERE True") ),
    ])
    def test_query_helper(self, is_date_sharded, use_legacy_sql, expected):
        table='test.test_table_'
        first_date_ts = 1546300800 if is_date_sharded else None
        last_date_ts = 1546473600 if is_date_sharded else None
        client = mock.Mock()
        helper = QueryHelper(table,
                             first_date_ts=first_date_ts,
                             last_date_ts=last_date_ts,
                             use_legacy_sql=use_legacy_sql,
                             test_client=client)
        assert encode_table_ref(helper.table_ref) == table
        assert helper.is_date_sharded == is_date_sharded
        expected = expected.format(table=table, first_date_ts=first_date_ts, last_date_ts=last_date_ts)
        assert helper.build_query() == expected
