"""Test ``tinyflow.serial.pipeline.Pipeline()`` execution."""


from collections import Counter
import itertools as it

import pytest

from tinyflow import exceptions, MapPipeline, ops, Pipeline


def test_wordcount():

    p = Pipeline() \
        | "Split line into words" >> ops.map(lambda x: x.lower().split()) \
        | "Create stream of words" >> ops.wrap(it.chain.from_iterable) \
        | "Count words and grab top 10" >> ops.wrap(
            lambda x: Counter(x).most_common(5)) \
        | "Sort by frequency desc" >> ops.sort(key=lambda x: x[1], reverse=True)

    with open('LICENSE.txt') as f:
        actual = dict(p(f))

    expected = {
        'the': 13,
        'of': 12,
        'or': 11,
        'and': 8,
        'in': 6,
    }

    assert expected == actual


def test_operation_parent_pipeline():

    p = Pipeline()

    class Op(ops.Operation):

        def __init__(self, check_pipeline=False):
            # Just referencing pipeline here will trigger an exception
            if check_pipeline:
                self.pipeline

        def __call__(self, stream):
            yield self.pipeline is p

    with pytest.raises(exceptions.NoPipeline):
        Op(check_pipeline=True)

    p |= Op()

    assert next(p([None])) is True


def test_MapPipeline_execution():

    square = MapPipeline() \
        | ops.map(lambda x: x ** 2)

    pipeline = Pipeline() \
        | ops.map(square) \
        | ops.flatten()

    data = tuple(range(5))
    assert tuple(pipeline(data)) == tuple(map(lambda x: x ** 2, data))


def test_subpipeline():

    square = Pipeline() \
        | ops.map(lambda x: x ** 2)

    pipeline = Pipeline() \
        | ops.map(lambda x: x - 1) \
        | square

    data = tuple(range(5))
    expected = tuple(map(lambda x: (x - 1) ** 2, data))
    actual = tuple(pipeline(data))
    assert expected == actual
