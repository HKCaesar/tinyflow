"""Tests for ``tinyflow.transform``."""


import itertools as it
import operator as op

from tinyflow.pipeline import Pipeline
import tinyflow.transform as t

import pytest


def test_default_description():
    tform = t.Transform()
    assert repr(tform) == tform.description


def test_description():
    tform = 'description' >> t.Transform()
    assert 'description' >> t.Transform()


def test_Filter():
    values = [0, 1, 2, None, 4]
    tform = t.Filter(bool)
    assert (1, 2, 4) == tuple(tform(values))


def test_Map():
    values = tuple(range(5))
    tform = t.Map(lambda x: x ** 2)
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
    tform = t.ReduceByKey(op.iadd)
    assert expected == dict(tform(data))


@pytest.mark.parametrize("reverse", [True, False])
def test_Sort(reverse):
    values = tuple(enumerate(range(10)))
    tform = t.Sort(key=lambda x: x[1], reverse=reverse)

    expected = tuple(sorted(values, key=lambda x: x[1], reverse=reverse))
    actual = tuple(tform(values))

    assert expected == actual


def test_Wrap(text):

    tform = t.Wrap(it.chain.from_iterable)

    expected = (line.split() for line in text.splitlines())
    expected = it.chain.from_iterable(expected)

    actual = (line.split() for line in text.splitlines())
    actual = tform(actual)


    for e, a in zip(expected, actual):
        assert e == a


def test_Transform_abc():
    tform = t.Transform()
    with pytest.raises(NotImplementedError):
        tform([])
