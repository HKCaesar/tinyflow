"""Tests for ``tinyflow.tools``."""


from tinyflow.tools import slicer

import pytest


def test_slicer_even():
    it = slicer(range(100), 10)
    for idx, actual in enumerate(it):

        assert isinstance(actual, tuple)
        assert len(actual) == 10

        # Verify that the values are correct
        assert actual == tuple((10 * idx) + i for i in range(len(actual)))

    assert idx == 9


def test_slicer_odd():

    it = slicer(range(5), 2)
    assert next(it) == (0, 1)
    assert next(it) == (2, 3)
    assert next(it) == (4, )
    with pytest.raises(StopIteration):
        next(it)