import pytest
import posixpath as pp
import json

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_tools.options import LoggingOptions
from pipe_tools.options import validate_options
from pipe_tools.options import ReadJSONAction



class TestOptions:
    def test_logging_options(self):
        args = ['--log_file=FILE']
        options = LoggingOptions(args)
        assert options.log_file == 'FILE'
        assert options.log_level == LoggingOptions.DEFAULT_LOG_LEVEL

    def test_configure_logging(self, temp_dir):
        log_file = pp.join(temp_dir, 'test.log')
        args = ['--log_file=%s' % log_file, '--log_args']
        options = LoggingOptions(args)
        options.configure_logging()

        with open(log_file) as f:
            log_data = f.read()

        assert log_file in log_data

    def test_validate_options(self):
        args=['--help']
        with pytest.raises(SystemExit) as e:
            validate_options(args, LoggingOptions)
        assert e.type == SystemExit
        assert e.value.code == 0


class JSONOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--json_arg',
            action=ReadJSONAction)

class TestJSONOptions:
    def test_read_json_action(self):
        d = dict(a = 1)
        args=['--json_arg=%s' % json.dumps(d)]
        options = JSONOptions(args)
        assert options.json_arg == d

    def test_read_json_file_action(self, temp_dir):
        d = dict(a = 1)
        filename = pp.join(temp_dir, 'test.json')
        with open(filename, 'w') as f:
            json.dump(d, f)

        args = ['--json_arg=@%s' % filename]
        options = JSONOptions(args)
        assert options.json_arg == d