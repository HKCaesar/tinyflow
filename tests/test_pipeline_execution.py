"""Test ``tinyflow.pipeline.Pipeline()`` execution."""


from collections import Counter
import itertools as it

from tinyflow.pipeline import Pipeline
from tinyflow import ops


def test_wordcount():

    p = Pipeline() \
        | "Split line into words" >> ops.Map(lambda x: x.lower().split()) \
        | "Create stream of words" >> ops.Wrap(it.chain.from_iterable) \
        | "Count words and grab top 10" >> ops.Wrap(
            lambda x: Counter(x).most_common(5)) \
        | "Sort by frequency desc" >> ops.Sort(key=lambda x: x[1], reverse=True)

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
