from apache_beam import DoFn
from apache_beam import PTransform
from apache_beam import ParDo




class DoNothing(PTransform):
    """
    A utility transform that does nothing, just passes elemented though unaltered
    """

    class DoNothingDoFn(DoFn):
        def process(self, element):
            yield element

    def expand(self, pcoll):
        return pcoll | ParDo(self.DoNothingDoFn())
