import pytest
import posixpath as pp


from pipe_tools.options import LoggingOptions
from pipe_tools.options import validate_options

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
