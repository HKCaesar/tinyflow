import abc
from collections import defaultdict, deque
from functools import reduce
import itertools as it
import operator as op


__all__ = [
    'Operation', 'map', 'wrap', 'reduce_by_key', 'sort', 'filter', 'flatten'
    'take', 'drop', 'itemgetter']


builtin_map = map
builtin_filter = filter


class _NULL(object):

    """A sentinel for when ``None`` is a valid value or default."""


class Operation(object):

    """Base class for developing pipeline steps."""

    @property
    def description(self):

        """A transform description can be added like:

            Pipeline() | "description" >> Operation()
        """

        return getattr(self, '_description', repr(self))

    @description.setter
    def description(self, value):

        """Subclassers use ``__init__()`` for arguments.  This is good
        enough.
        """

        self._description = value

    @abc.abstractmethod
    def __call__(self, stream):

        """Given a stream of data, apply the transform.

        Parameters
        ----------
        stream : iter
            Apply transform to the stream of data.

        Yields
        ------
        object
            Operationed objects.
        """

        raise NotImplementedError

    def __rrshift__(self, other):

        """Add a description to this pipeline phase."""

        self.description = other
        return self


class map(Operation):

    """Map a function across the stream of data."""

    def __init__(self, func):

        """
        Parameters
        ----------
        func : function
            Map this function.
        """

        self.func = func

    def __call__(self, stream):
        return builtin_map(self.func, stream)


class flatmap(map):

    """Like: ``map(func) | flatten()``."""

    def __call__(self, stream):
        return it.chain.from_iterable(super().__call__(stream))


class wrap(Operation):

    """Wrap the data stream in an arbitrary transform.

    For example:

        Pipeline() | Wrap(itertools.chain.from_iterable)
    """

    def __init__(self, func):

        """
        Parameters
        ----------
        func : function
            Wrap the data stream with this function.
        """

        self.func = func

    def __call__(self, stream):
        return self.func(stream)


class reduce_by_key(Operation):

    """Partition the data stream by key and reduce each partition to a single
    value.  Expects data to be a stream like:

        (key1, value)
        (key2, value)

    This could be used in a wordcount style computation:

        import itertools as it
        import operator as op

        from tinyflow.pipeline import Pipeline
        from tinyflow import transform as t

        p = Pipeline() \
            | "Split lines into words" >> t.Map(lambda x: x.split()) \
            | "Create a stream of words" >> t.Wrap(it.chain.from_iterable) \
            | "Apply a value to each word" >> t.Map(lambda x: (x, 1)) \
            | "Compute word frequency" >> t.ReduceByKey(op.iadd)

        with open('file.txt') as f:
            results = dict(p(f))
    """

    def __init__(self, func):

        """
        Parameters
        ----------
        func : function
            Like ``operator.iadd()``.  Anything that fulfills the function
            argument for ``functools.reduce()``.
        """

        self.func = func

    def __call__(self, stream):
        partitioned = defaultdict(deque)
        for key, value in stream:
            partitioned[key].append(value)
        yield from (
            (k, reduce(self.func, v)) for k, v in partitioned.items())


class sort(Operation):

    """Sort the stream of data.  Just a wrapper around ``sorted()``."""

    def __init__(self, **kwargs):

        """
        Parameters
        ----------
        kwargs : **kwargs
            Keyword arguments for ``sorted()``.
        """

        self.kwargs = kwargs

    def __call__(self, stream):
        return sorted(stream, **self.kwargs)


class filter(Operation):

    """Filter the data stream.  Keeps elements that evaluate as ``True``."""

    def __init__(self, func):

        """
        Parameters
        ----------
        func : function
            See ``filtered()``'s documentation.
        """

        self.func = func

    def __call__(self, stream):
        return builtin_filter(self.func, stream)


class flatten(Operation):

    """Flatten an iterable.  Like ``itertools.chain.from_iterable()``."""

    def __call__(self, stream):
        return it.chain.from_iterable(stream)


class take(Operation):

    """Take N items from the stream."""

    def __init__(self, count):

        """
        Parameters
        ----------
        count : int
            Take this many items.
        """

        self.count = count

    def __call__(self, stream):
        return it.islice(stream, self.count)


class drop(Operation):

    """Drop N items from the stream."""

    def __init__(self, count):

        """
        Parameters
        ----------
        count : int
            Drop this many items.
        """

        self.count = count

    def __call__(self, stream):
        stream = iter(stream)
        for _ in range(self.count):
            next(stream)
        return stream


class itemgetter(Operation):

    """Like ``itertools.itemgetter()``."""

    def __init__(self, *args, **kwargs):

        """
        Parameters
        ----------
        args : *args
            For ``operator.itemgetter()``.
        kwargs : **kwargs
            For ``operator.itemgetter()``.
        """

        self.getter = op.itemgetter(*args, **kwargs)

    def __call__(self, stream):
        return builtin_map(self.getter, stream)
