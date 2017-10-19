import pytest
import pytz

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from datetime import datetime
from pipe_tools.timestamp import *
from pipe_tools.generator import MessageGenerator


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestGenerator():

    def test_message_generator(self):

        message_generaator = MessageGenerator()
        messages = list(message_generaator)

        assert len(messages) == message_generaator.count
        assert messages[0]['timestamp'] == message_generaator.start_ts

