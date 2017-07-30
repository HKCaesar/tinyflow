"""Tests for ``tinyflow.serial.pipeline``."""


import pytest

from tinyflow import Pipeline
from tinyflow.exceptions import NotAnOperation


def test_pipeline_exceptions():
    p = Pipeline()
    with pytest.raises(NotAnOperation):
        p | None


def test_Pipeline_subclass_close():
    class P(Pipeline):
        def __init__(self):
            self.closed = False
        def close(self):
            self.closed = True
    with P() as p:
        assert not p.closed
    assert p.closed
