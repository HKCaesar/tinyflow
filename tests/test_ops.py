"""Tests for ``tinyflow.ops``."""


from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import operator as op

import pytest

from tinyflow import _testing, exceptions, ops, Pipeline, tools


def test_default_description():
    tform = ops.flatten()
    assert repr(tform) == tform.description


def test_description():
    tform = 'description' >> ops.flatten()
    assert tform.description == 'description'


def test_Operation_abc():
    """An operation without ``__call__()`` is invalid."""
    with pytest.raises(NotImplementedError):
        ops.Operation()([])


@pytest.mark.parametrize("pool", ['thread', 'process'])
def test_Operation_no_pool(pool):

    class Op(ops.Operation):

        def __init__(self, pool):
            self._pool = pool

        def __call__(self, stream):
            if self._pool == 'process':
                self.pipeline.process_pool
            elif self._pool == 'thread':
                self.pipeline.thread_pool
            else:
                raise RuntimeError('uh ...')

    p = Pipeline() | Op(pool)
    with pytest.raises(exceptions.NoPool):
        p([])


def test_Operation_no_pipeline():

    class Op(ops.Operation):

        def __init__(self):
            self.pipeline

        def __call__(self, stream):
            pass

    with pytest.raises(exceptions.NoPipeline):
        Op()


@pytest.mark.parametrize("operation,input_data,expected", [
    (ops.counter(3), (1, 1, 2, 2, 4, 4, 5), ((1, 2), (2, 2), (4, 2))),
    (ops.counter(), (1, 1, 2, 2, 4, 4, 5), ((1, 2), (2, 2), (4, 2), (5, 1))),
    (ops.drop(2), range(5), (2, 3, 4)),
    (ops.take(2), range(5), (0, 1)),
    (ops.map(lambda x: x ** 2), (2, 4, 8), (4, 16, 64)),
    (ops.map(lambda x: x.upper(), flatten=True), ['w1', 'w2'], ['W', '1', 'W', '2']),
    (ops.flatten(), ((1, 2), (3, 4)), (1, 2, 3, 4)),
    (ops.filter(bool), (0, 1, 2, None, 4), (1, 2, 4)),
    (ops.filter(), (0, 1, 2, None, 4), (1, 2, 4)),
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


def _parametrize_test_map_star_args(pools, args):

    """Prepare parametrized arguments for ``test_map_star_args()`` to make
    sure everything is tested across all pool types.

    Takes args like:

        (_testing.add2, '*args', [(1, 2), (3, 4)], [3, 7])

    and pools like:

        [(ProcessPoolExecutor, 'process'), (ThreadPoolExecutor, 'thread')]

    and produces:

        (_testing.add2, '*args', [(1, 2), (3, 4)], [3, 7], ProcessPoolExecutor, 'process')
        (_testing.add2, '*args', [(1, 2), (3, 4)], [3, 7], ThreadPoolExecutor, 'thread')
    """

    for item in args:
        for p in pools:
            yield tuple(list(item) + list(p))


@pytest.mark.parametrize("func,argtype,data,expected,pool_class,pool_name",
    list(_parametrize_test_map_star_args(
        pools=[(ProcessPoolExecutor, 'process'), (ThreadPoolExecutor, 'thread')],
        args=[
            (_testing.add2, '*args', [(1, 2), (3, 4)], [3, 7]),
            (_testing.add2, '**kwargs', [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}], [3, 7]),
            (_testing.add4, '*args**kwargs', [((1, 2), {'c': 3, 'd': 4}), ((5, 6), {'c': 7, 'd': 8})], [10, 26])
        ])))
def test_map_arg_types_and_pools(
        func, argtype, data, expected, pool_class, pool_name):

    """Every argument type should work with every pool type."""

    p = Pipeline() | ops.map(func, argtype=argtype, pool=pool_name)
    with pool_class(4) as pool:
        kwargs = {'{}_pool'.format(pool_name): pool}
        actual = p(data, **kwargs)
        # Cast both outputs to list and sort actual since it may be out of
        # order after mapping in parallel.
        assert list(expected) == sorted(actual)


def test_map_exceptions():
    # Bad argtype
    with pytest.raises(ValueError):
        ops.map(lambda x: x, argtype=None)
    # Bad pool
    p = Pipeline() | ops.map(lambda x: x, pool='trash')
    with pytest.raises(ValueError):
        p([])
