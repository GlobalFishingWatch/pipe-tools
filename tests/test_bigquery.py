import pytest

from pipe_tools.io.bigquery import encode_table_ref
from pipe_tools.io.bigquery import decode_table_ref


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestBigquery:

    @pytest.mark.parametrize("table", [
        "Project:dataset.table",
        "dataset.table"
    ])
    def test_tableref_encode(self, table):
        assert encode_table_ref(decode_table_ref(table)) == table
