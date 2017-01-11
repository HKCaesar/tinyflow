"""Pipeline and concurrency tools.  This will be migrated to ``async/await``
at some point.  In its current state ``Pipeline.__call__()`` needs a coroutine
scheduler.
"""


import abc


def coroutine(func):

    """Decorator for priming a coroutine.

        @coroutine
        def printer():
            while True:
                line = yield
                print(line)

    Prevents having to prime the coroutine with:

        coro = printer()
        coro.send(None)
        for item in range(10):
            coro.send(item)

    Instead the coroutine is primed by the decorator and is immediately ready
    to use:

        coro = printer()
        for item in range(10):
            coro.send(item)
    """

    def prime_coroutine(*args, **kwargs):
        c = func(*args, **kwargs)
        c.send(None)
        return c
    return prime_coroutine


@coroutine
def printer():

    """A coroutine that just prints objects that are pushed into it.  Useful
    for debugging.
    """

    while True:
        item = yield
        print(item)


@coroutine
def collect(queue):

    """A coroutine for collecting results into a queue.

    Parameters
    ----------
    queue : list or deque or Queue
        Must support ``queue.append()``.
    """

    while True:
        item = yield
        queue.append(item)


class NotACoroTransform(Exception):
    
    """``CoroPipeline()`` can only operate on ``CoroTransforms()``."""


class CoroPipeline(object):
    
    """Concurrent pipeline."""

    def __init__(self, sink=printer()):

        """
        The ``sink`` parameter is the final output at the end of the pipeline.

        Parameters
        ----------
        sink : function
            Returning a coroutine.
        """

        self.transforms = []
        self.sink = sink

    def __or__(self, other):

        """
        Parameter
        ---------
        other : CoroTransform
            A pipeline transform.
        """

        if not isinstance(other, CoroTransform):
            raise NotACoroTransform(
                f"Can only pipe data into an instance of 'Transform()', not: "
                f"'{other}'")
        self.transforms.append(other)
        return self

    __ior__ = __or__

    def __call__(self, stream):

        """Process the input data stream.

        Parameters
        ----------
        stream : iter
            Input data.
        """

        target = self.sink
        for trans in reversed(self.transforms):
            target = trans(target)
        for item in stream:
            target.send(item)
        target.close()


class CoroTransform(object):

    """Base class for coroutine aware transforms."""

    @abc.abstractmethod
    def __call__(self, target):

        """Process data and send it on to the next coroutine.

        Parameter
        ---------
        target : types.CoroutineType
            Send data on to this phse of the pipeline.
        """

        raise NotImplementedError

    @property
    def description(self):

        """A transform description can be added like:

            CoroPipeline() | "description" >> CoroTransform()
        """

        return getattr(self, '_description', repr(self))

    @description.setter
    def description(self, value):

        """Subclassers use ``__init__()`` for arguments.  This allows for
        for setting/storing the description.
        """

        self._description = value

    def __rrshift__(self, other):

        """Add a description to this pipeline phase."""

        self.description = other
        return self


class Map(CoroTransform):

    """Like ``map()``."""

    def __init__(self, func):
        self.func = func

    @coroutine
    def __call__(self, target):
        while True:
            data = yield
            target.send(self.func(data))


class FlatMap(Map):

    """Like ``itertools.chain.from_iterable(map())``."""

    @coroutine
    def __call__(self, target):
        while True:
            data = yield
            for item in self.func(data):
                target.send(item)


class Filter(CoroTransform):

    """Like ``filter()``."""

    def __init__(self, func=bool):
        self.func = func

    @coroutine
    def __call__(self, target):
        while True:
            data = yield
            if self.func(data):
                target.send(data)


class FilterFalse(CoroTransform):

    """Like ``itertools.filterfalse()``"""

    def __init__(self, func):
        self.func = func

    @coroutine
    def __call__(self, target):
        while True:
            data = yield
            if not self.func(data):
                target.send(data)


class Sort(CoroTransform):

    """Like ``sorted()``."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @coroutine
    def __call__(self, target):
        out = []
        try:
            while True:
                data = yield
                out.append(data)
        except GeneratorExit:
            for item in sorted(out, **self.kwargs):
                target.send(item)
            target.close()


class ReduceByKey(CoroTransform):

    """Split data stream by key and reduce all data for each key to a single
    value.  This pipeline computes the sum of all odd and even numbers in
    a range of values:

        p = CoroPipeline() \
            | "Pdd or even?" >> Map(lambda x: ('even' if x % 2 else 'odd', x) \
            | "Compute sum" >> ReduceByKey(lambda key, a, b: a + b)

    and this code does the same:

        from collections import defaultdict
        from functools import reduce

        data = range(10)
        partitioned = defaultdict(list)

        for value in data:
            if key % 2 == 0:
                key = 'even'
            else:
                key = 'odd'
            partitioned[key].append(value

        results = {k: reduce(op.iadd, v) for k, v in partitioned.items()}
        """

    def __init__(self, func):
        self.func = func

    @coroutine
    def wrap(self, key, func, target):
        a = yield
        try:
            while True:
                b = yield
                a = func(key, a, b)
        except GeneratorExit:
            # Don't close target!  It is handled elsewhere.
            target.send((key, a))

    @coroutine
    def __call__(self, target):
        keymap = {}
        try:
            while True:
                key, value = yield
                tgt = keymap.get(key)
                if not tgt:
                    tgt = self.wrap(key, self.func, target)
                    keymap[key] = tgt
                tgt.send(value)
        except GeneratorExit:
            # Downstream transforms may be streaming.  Reduce memory usage.
            while keymap:
                _, tgt = keymap.popitem()
                tgt.close()
            target.close()


class Sample(CoroTransform):

    """Slice stream with ``Slice(start, stop, step)``.  Like
    ``list[start:stop:step]``.
    """

    def __init__(self, *args):

        """Accepts arguments in the same manner as ``range()`` or ``slice()``.

        Parameters
        ----------
        start : int, optional
        stop : int
        step : int, optional
        """

        if len(args) == 1:
            self.start = 0
            self.stop = args[0]
            self.step = 1
        elif len(args) == 2:
            self.start, self.stop = args
            self.step = 1
        elif len(args) == 3:
            self.start, self.stop, self.step = args
        else:
            raise TypeError(f"Slice() expected 1 arguments, got {len(args)}")

    @coroutine
    def __call__(self, target):
        matches = iter(range(self.start, self.stop, self.step))
        idx = 0
        while True:
            data = yield
            if next(matches) == idx:
                target.send(data)
            idx += 1
