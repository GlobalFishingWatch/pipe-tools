import pytest
import posixpath as pp

from pipe_tools.cookbook import pipeline_options


#TODO:  figure out how to test the ones that write to biguery
# in the mean time, there is a script runall.sh in the cookbook dir that you can use to quickly run them all

@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
class TestCookbook():
    def test_pipeline_options(self, temp_dir):
        out_dir=pp.join(temp_dir, "pipeline_options")
        args=[
            "--count=100",
            "--output-file-prefix=%s" % pp.join(out_dir, "shard")
        ]
        exit_code =  pipeline_options.run(args=args)
        assert exit_code == 0


    def test_write_date_partitions(self, temp_dir):
        out_dir = pp.join(temp_dir, "write_date_partitions")
        args = [
            "--count=100",
            "--output-file-prefix=%s" % pp.join(out_dir, "shard")
        ]
        exit_code = pipeline_options.run(args=args)
        assert exit_code == 0
