import abc
from collections import Counter, defaultdict, deque
from functools import reduce
import itertools as it
import operator as op

from tinyflow import tools


__all__ = [
    'Operation', 'map', 'wrap', 'sort', 'filter',
    'flatten' 'take', 'drop', 'itemgetter', 'windowed_op',
    'windowed_reduce', 'flatmap', 'counter']


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


class sort(Operation):

    """Sort the stream of data.  Just a wrapper around ``sorted()``."""

    def __init__(self, key=None, reverse=False):

        """
        Parameters
        ----------
        key : callable or None, optional
            Key function for ``sorted()``.
        reverse : bool, optional
            For ``sorted()``.
        """

        self.key = key
        self.reverse = reverse

    def __call__(self, stream):
        return sorted(stream, key=self.key, reverse=self.reverse)


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


class windowed_op(Operation):

    """Windowed operations.  Group ``count`` items and hand off to
    ``operation``.  Items are yielded from ``operation`` individually
    back into the stream.
    """

    def __init__(self, count, operation):

        """
        Parameters
        ----------
        count : int
            Apply ``operation`` across a group of ``count`` items.
        operation : callable
            Apply this function to groups of items.
        """

        self.count = count
        self.operation = operation

    def __call__(self, stream):
        for window in tools.slicer(stream, self.count):
            yield from self.operation(window)


class windowed_reduce(Operation):

    """Group N items together into a window and reduce to a single value."""

    def __init__(self, count, reducer):

        """
        Parameters
        ----------
        count : int
            Window size.
        reducer : function
            Like ``operator.iadd()``.
        """

        self.count = count
        self.reducer = reducer

    def __call__(self, stream):
        for window in tools.slicer(stream, self.count):
            yield reduce(self.reducer, window)


class counter(Operation):

    """Count items and optionally produce only the N most common."""

    def __init__(self, most_common=None):

        """
        Parameters
        ----------
        most_common : int or None, optional
            Only emit the N most common items.
        """

        self.most_common = most_common

    def __call__(self, stream):
        frequency = Counter(stream)
        if self.most_common:
            results = frequency.most_common(self.most_common)
        else:
            results = frequency.items()
        return iter(results)
