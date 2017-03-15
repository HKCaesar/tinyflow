"""Test ``tinyflow.serial.pipeline.Pipeline()`` execution."""


from collections import Counter
import itertools as it

from tinyflow import ops, Pipeline


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
