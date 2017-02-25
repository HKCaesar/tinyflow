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
    assert 'description' >> ops.Operation()


def test_Filter():
    values = [0, 1, 2, None, 4]
    tform = ops.filter(bool)
    assert (1, 2, 4) == tuple(tform(values))


def test_Map():
    values = tuple(range(5))
    tform = ops.map(lambda x: x ** 2)
    assert tuple(map(lambda x: x ** 2, values)) == tuple(tform(values))


def test_ReduceByKey():

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
def test_Sort(reverse):
    values = tuple(enumerate(range(10)))
    tform = ops.sort(key=lambda x: x[1], reverse=reverse)

    expected = tuple(sorted(values, key=lambda x: x[1], reverse=reverse))
    actual = tuple(tform(values))

    assert expected == actual


def test_Wrap(text):

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
