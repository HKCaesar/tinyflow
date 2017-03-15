"""Pipeline operations for processing data serially."""


import abc
from collections import Counter
import copy
from functools import reduce
import itertools as it

from tinyflow import tools


__all__ = [
    'Operation', 'map', 'wrap', 'sort', 'filter',
    'flatten' 'take', 'drop', 'windowed_op',
    'windowed_reduce', 'flatmap', 'counter', 'reduce_by_key']


builtin_map = map
builtin_filter = filter


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

    def __init__(self, func, flatten=False):

        """
        Parameters
        ----------
        func : function
            Map this function.
        flatten : bool, optional
            Like: ``itertools.chain.from_iterable(map(<func>, <iterable>))``.
        """

        self.func = func
        self.flatten = flatten

    def __call__(self, stream):
        results = builtin_map(self.func, stream)
        if self.flatten:
            results = it.chain.from_iterable(results)
        return results


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


class reduce_by_key(Operation):

    """Apply a reducer by key.  Holds all keys plus one value for each key
    in memory until items are emitted.

    Given a stream of words this would produce a sequence of
    ``(word, frequency)`` tuples:

        import operator

        reduce_by_key(
            operator.iadd,
            keyfunc=lambda x: x,
            valfunc=lambda x: 1)

    Values are emitted as a sequence of ``(key, val)`` tuples.
    """

    def __init__(
            self, reducer, keyfunc, valfunc=lambda x: x, initial=tools.NULL,
            copy_initial=False, deepcopy_initial=False):

        """
        Parameters
        ----------
        reducer : callable
            Function for reducing values.  Must take two values and return
            one, like ``operator.iadd()`` or: ``lambda a, b: a + b``.
        keyfunc : callable
            Function for extracting the key from every input item.
        valfunc : callable, optional
            Function for extracting the value from every input item.  If not
            given the item itself is used as the value.
        initial : object, optional
            Reduce the first value for each key against this initial value,
            like the ``initial`` parameter for ``functools.reduce()`` but
            ``None`` is valid.
        copy_initial : bool, optional
            Pass ``initial`` through ``copy.copy()`` before use.
            Prevents mutable objects from being altered by every key at the
            cost of some overhead.  Cannot be combined with
            ``deepcopy_initial``.
        deepcopy_initial : bool, optional
            Same as ``copy_initial`` but with ``copy.deepcopy()``.  Cannot
            be combined with ``copy_initial``.
        """

        self.reducer = reducer
        self.keyfunc = keyfunc
        self.valfunc = valfunc
        self.initial = initial
        self.copy_initial = copy_initial
        if copy_initial and deepcopy_initial:
            raise ValueError(
                "Cannot combine 'copy_initial' and 'deepcopy_initial'.")
        elif copy_initial:
            self.copier = copy.copy
        elif deepcopy_initial:
            self.copier = copy.deepcopy
        else:
            self.copier = lambda x: x

    def __call__(self, stream):
        partitioned = {}

        # Add keys to the stream and extract values
        stream = ((self.keyfunc(i), self.valfunc(i)) for i in stream)

        for key, value in stream:

            p_value = partitioned.get(key, tools.NULL)

            # This key already exists.  Reduce.
            if p_value != tools.NULL:
                partitioned[key] = self.reducer(p_value, value)

            # New key and no initial value.  Just insert the value from the
            # stream.
            elif p_value == tools.NULL and self.initial == tools.NULL:
                partitioned[key] = value

            # New key and an initial value
            elif self.initial != tools.NULL:
                partitioned[key] = self.reducer(
                    self.copier(self.initial), value)

            else:  # pragma: no cover
                raise ValueError("This shouldn't happen.")

        while partitioned:
            yield partitioned.popitem()
