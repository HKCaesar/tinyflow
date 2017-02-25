"""Tests for ``tinyflow.ops``."""


import copy
import operator as op

from tinyflow.serial import ops

import pytest


def test_default_description():
    tform = ops.Operation()
    assert repr(tform) == tform.description


def test_description():
    tform = 'description' >> ops.Operation()
    assert tform.description == 'description'


def test_Operation_abc():
    o = ops.Operation()
    with pytest.raises(NotImplementedError):
        o([])


@pytest.mark.parametrize("operation,input_data,expected", [
    (ops.itemgetter(0), (('something', None), ('none', 'else')), ('something', 'none')),
    (ops.drop(2), range(5), (2, 3, 4)),
    (ops.take(2), range(5), (0, 1)),
    (ops.map(lambda x: x ** 2), (2, 4, 8), (4, 16, 64)),
    (ops.flatmap(lambda x: x.upper()), ['w1', 'w2'], ['W', '1', 'W', '2']),
    (ops.flatten(), ((1, 2), (3, 4)), (1, 2, 3, 4)),
    (ops.filter(bool), (0, 1, 2, None, 4), (1, 2, 4)),
    (ops.wrap(reversed), (1, 2, 3), (3, 2, 1)),
    (ops.windowed_reduce(2, op.iadd), (1, 2, 3, 4, 5), (3, 7, 5)),
    (ops.windowed_op(2, reversed), (1, 2, 3, 4, 5), (2, 1, 4, 3, 5)),
    (ops.sort(), (2, 3, 1), (1, 2, 3)),
    (ops.sort(reverse=True), (2, 3, 1), (3, 2, 1)),
    # Complex sorting with a key function and reversed.  Data is a series
    # of tuples.  Sort on the first element of each tuple.
    (
        ops.sort(key=op.itemgetter(0), reverse=True),
        ((1, 'dog'), (3, 'cat'), (2, 'fish')),
        ((3, 'cat'), (2, 'fish'), (1, 'dog')))
    ])
def test_basic_operation(operation, input_data, expected):

    """A lot of operations take few arguments are generate an only slightly
    altered output.  Output and expected values are compared as tuples.
    """

    assert isinstance(operation, ops.Operation)
    assert tuple(operation(input_data)) == tuple(expected)
