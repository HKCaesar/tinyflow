"""Tests for ``tinyflow.ops``."""


import operator as op

import pytest

from tinyflow import ops, tools


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
    (ops.counter(3), (1, 1, 2, 2, 4, 4, 5), ((1, 2), (2, 2), (4, 2))),
    (ops.counter(), (1, 1, 2, 2, 4, 4, 5), ((1, 2), (2, 2), (4, 2), (5, 1))),
    (ops.drop(2), range(5), (2, 3, 4)),
    (ops.take(2), range(5), (0, 1)),
    (ops.map(lambda x: x ** 2), (2, 4, 8), (4, 16, 64)),
    (ops.map(lambda x: x.upper(), flatten=True), ['w1', 'w2'], ['W', '1', 'W', '2']),
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


def test_reduce_by_key_exceptions():
    with pytest.raises(ValueError):
        ops.reduce_by_key(
            None, None, copy_initial=True, deepcopy_initial=True)


@pytest.mark.parametrize('initial', [tools.NULL, 0, 10])
def test_reduce_by_key_wordcount(initial):

    """Tests ``keyfunc``, ``valfunc``, and ``initial``."""

    data = ['word', 'something', 'else', 'word']
    expected = {
        'word': 2,
        'something': 1,
        'else': 1
    }
    if initial != tools.NULL:
        expected = {k: v + initial for k, v in expected.items()}
    o = ops.reduce_by_key(
        op.iadd,
        lambda x: x,
        valfunc=lambda x: 1,
        initial=initial)

    assert expected == dict(o(data))


@pytest.mark.parametrize('kwargs', (
        {'copy_initial': True},
        {'deepcopy_initial': True}))
def test_reduce_by_key_initial_copier(kwargs):

    data = [
        ['key1', 1],
        ['key', 1],
        ['key', 2],
    ]
    expected = {
        'key1': [None, 'key1', 1],
        'key': [None, 'key', 1, 'key', 2]
    }
    o = ops.reduce_by_key(
        op.iadd,
        op.itemgetter(0),
        initial=[None],
        **kwargs)
    actual = dict(o(data))
    assert expected == actual


# def test_parallel_map():
#