"""Pipeline model.

A word count example using a threadpool, a sub-pipeline, and a mapped
pipeline to count words in multiple files across multiple threads while
consuming a very small amount of memory:

    from concurrent.futures import ThreadPoolExecutor
    import operator as op

    from tinyflow import ops, MapPipeline, Pipeline


    # Compute word count for a single file
    wordcount = MapPipeline() \
        | ops.cat() \
        | ops.methodcaller('lower') \
        | ops.methodcaller('split') \
        | ops.filter() \
        | ops.flatten() \
        | ops.counter()

    # Summarize stats across all files
    aggregate_wordcount = Pipeline() \
        | ops.flatten() \
        | ops.reduce_by_key(op.iadd, op.itemgetter(0), op.itemgetter(1))

    # Distributes the process across threads, aggregates, sorts, and
    # determines the 10 most frequent words.
    pipeline = Pipeline() \
       | ops.map(wordcount, pool='thread') \
       | aggregate_wordcount \
       | ops.sort(op.itemgetter(1), reverse=True) \
       | ops.take(10)

    # Execute the pipeline and give it access to 4 threads.
    infiles = ('LICENSE.txt' for _ in range(2))
    with ThreadPoolExecutor(4) as threads:
        for item in pipeline(infiles, thread_pool=threads):
            print(item)
"""


from .exceptions import NoPool, NotAnOperation
from .ops import Operation


__all__ = ['MapPipeline', 'Pipeline']


class Pipeline(object):

    """A ``tinyflow`` pipeline model.  Subclass to attach your own custom
    ``__init__()`` init.

        from tinyflow import ops, Pipeline


        pipeline = Pipeline() \
            | ops.map(lambda x: x ** 2)

        for item in pipeline(range(10)):
            pass

    Pipelines can also be treated as operations:

        from tinyflow import ops, Pipeline


        square_pipeline = Pipeline() \
            | ops.map(lambda x: x ** 2)

        pipeline = Pipeline() \
            | ops.map(lambda x: x - 1) \
            | square_pipeline

        for item in pipeline(range(10)):
            pass


    Attributes
    ----------
    operations : tuple
        Instances of ``tinyflow.ops.Operation()`` that will be used to process
        data.
    thread_pool : concurrent.futures.ThreadPoolExecutor or None
        Will be ``None`` unless a thread pool was passed to
        ``Pipeline.__call__()``.
    process_pool : concurrent.futures.ProcessPoolExecutor or None
        Will be ``None`` unless a process pool was passed to
        ``Pipeline.__call__()``.
    """

    @property
    def operations(self):
        return getattr(self, '_operations', tuple())

    @property
    def thread_pool(self):
        pool = getattr(self, '_thread_pool', None)
        if pool is None:
            raise NoPool(
                "An operation requested a thread pool but {!r} did not "
                "receive one.".format(self))
        return pool

    @property
    def process_pool(self):
        pool = getattr(self, '_process_pool', None)
        if pool is None:
            raise NoPool(
                "An operation requested a process pool but {!r} did not "
                "receive one.".format(self))
        return pool

    def close(self):
        """Override if to teardown a pipeline in ``Pipeline.__exit__()``."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __or__(self, other):

        """Add a ``tinyflow.ops.Operation()`` to the pipeline."""

        if not isinstance(other, (Operation, Pipeline)):
            raise NotAnOperation(
                "Expected an 'Operation()', not: {}".format(other))
        other.pipeline = self

        # Enforce immutability when calling 'Pipeline.operations'
        self._operations = tuple(list(self.operations) + [other])

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

        data = iter(data)

        self._process_pool = process_pool
        self._thread_pool = thread_pool

        for op in self.operations:
            # Ensure downstream nodes get an ambiguous iterator and not
            # something like a list that they get hooked on abusing.
            data = iter(op(data))

        return data


class MapPipeline(Pipeline):

    """Like ``Pipeline()`` except ``__call__()`` expects a single object
    instead of an iterable.  This is to enable mapping entire pipelines
    across a stream controlled by a different pipeline.  For instance,
    this produces a word count derived from multiple files across multiple
    threads:

        from tinyflow import MapPipeline, ops, Pipeline

        wordcount = MapPipeline() \
            | ops.cat() \
            | ops.methodcaller('lower') \
            | ops.methodcaller('split') \
            | ops.filter() \
            | ops.flatten() \
            | ops.counter()

        pipeline = Pipeline() \
            | ops.map(wordcount, pool='thread') \
            | ops.flatten() \
            | ops.reduce_by_key(op.iadd, op.itemgetter(0), op.itemgetter(1)) \

        for word, count in pipeline(<infiles>):
            pass
    """

    def __call__(self, data, *args, **kwargs):
        return iter(super(MapPipeline, self).__call__(
            iter([data]), *args, **kwargs))
