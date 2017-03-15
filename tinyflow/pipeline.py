"""Pipeline model."""


from .exceptions import NotAnOperation
from .ops import Operation


class Pipeline(object):

    """A ``tinyflow`` pipeline model."""

    def __init__(self):
        self.transforms = []
        self.thread_pool = None
        self.process_pool = None

    def __or__(self, other):

        """Add a transform to the pipeline."""

        if not isinstance(other, Operation):
            raise NotAnOperation(
                "Expected an 'Operation()', not: {}".format(other))
        other.pipeline = self
        self.transforms.append(other)
        return self

    __ior__ = __or__

    def __call__(self, data, process_pool=None, thread_pool=None):

        """Stream data through the pipeline."""

        self.process_pool = process_pool
        self.thread_pool = thread_pool

        for trans in self.transforms:
            # Ensure downstream nodes get an ambiguous iterator and not
            # something like a list that they get hooked on abusing.
            data = trans(data)

        return data
