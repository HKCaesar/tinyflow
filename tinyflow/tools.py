"""Assorted tools for working with streaming data."""


import itertools as it


class NULL(object):

    """A sentinel for when ``None`` is a valid value or default."""


def slicer(iterable, chunksize):

    """
    Read an iterator in chunks.
    Example:
        >>> for p in slicer(range(5), 2):
        ...     print(p)
        (0, 1)
        (2, 3)
        (4,)
    Parameters
    ----------
    iterable : iter
        Input stream.
    chunksize : int
        Number of records to include in each chunk.  The last chunk will be
        incomplete unless the number of items in the stream is evenly
        divisible by `size`.
    Yields
    ------
    tuple
    """

    iterable = iter(iterable)
    while True:
        v = tuple(it.islice(iterable, chunksize))
        if v:
            yield v
        else:
            raise StopIteration
