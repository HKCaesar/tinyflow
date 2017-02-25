"""Tests for ``tinyflow.ops``."""


import itertools as it
import operator as op

from tinyflow import ops

import pytest


def test_default_description():
    tform = ops.Operation()
    assert repr(tform) == tform.description


def test_description():
    tform = 'description' >> ops.Operation()
    assert tform.description == 'description'


def test_reduce_by_key():

    data = [
        ('key', 1),
        ('key2', 1),
        ('key', 1),
        ('other', 1)
    ]
    expected = {
        'key': 2,
        'key2': 1,
        'other': 1
    }
    tform = ops.reduce_by_key(op.iadd)
    assert expected == dict(tform(data))


@pytest.mark.parametrize("reverse", [True, False])
def test_sort(reverse):
    values = tuple(enumerate(range(10)))
    tform = ops.sort(key=lambda x: x[1], reverse=reverse)

    expected = tuple(sorted(values, key=lambda x: x[1], reverse=reverse))
    actual = tuple(tform(values))

    assert expected == actual


def test_wrap(text):

    tform = ops.wrap(it.chain.from_iterable)

    expected = (line.split() for line in text.splitlines())
    expected = it.chain.from_iterable(expected)

    actual = (line.split() for line in text.splitlines())
    actual = tform(actual)

    for e, a in zip(expected, actual):
        assert e == a


def test_Operation_abc():
    o = ops.Operation()
    with pytest.raises(NotImplementedError):
        o([])


@pytest.mark.parametrize("operation,input_data,expected", [
    (ops.drop(2), range(5), (2, 3, 4)),
    (ops.take(2), range(5), (0, 1)),
    (ops.flatmap(lambda x: x.upper()), ['w1', 'w2'], ['W', '1', 'W', '2']),
    (ops.map(lambda x: x ** 2), (2, 4, 8), (4, 16, 64)),
    (ops.filter(bool), (0, 1, 2, None, 4), (1, 2, 4))
])
def test_basic_operation(operation, input_data, expected):

    """A lot of operations take few arguments are generate an only slightly
    altered output.  Output and expected values are compared as tuples.
    """

    assert tuple(operation(input_data)) == tuple(expected)
