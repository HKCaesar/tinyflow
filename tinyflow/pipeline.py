"""Pipeline model."""


from .exceptions import NoPool, NotAnOperation
from .ops import Operation


class Pipeline(object):

    """A ``tinyflow`` pipeline model."""

    def __init__(self):
        self.transforms = []
        self._thread_pool = None
        self._process_pool = None

    @property
    def thread_pool(self):
        if self._thread_pool is None:
            raise NoPool(
                "An operation requested a thread pool but {!r} did not "
                "receive one.".format(self))
        return self._thread_pool

    @property
    def process_pool(self):
        if self._process_pool is None:
            raise NoPool(
                "An operation requested a process pool but {!r} did not "
                "receive one.".format(self))
        return self._process_pool

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

        """Stream data through the pipeline.

        Parameters
        ----------
        data : object
            Input data.  Most operations expect an iterable, but some
            do not.
        process_pool : None or concurrent.futures.ProcessPoolExecutor
            A process pool that individual operations can use if needed.
        thread_pool : None or concurrent.futures.ThreadPoolExecutor
            A thread pool that individual operations can use if needed.
        """

        self._process_pool = process_pool
        self._thread_pool = thread_pool

        for trans in self.transforms:
            # Ensure downstream nodes get an ambiguous iterator and not
            # something like a list that they get hooked on abusing.
            data = trans(data)

        return data
