"""Tests for ``tinyflow.serial.pipeline``."""


import pytest

from tinyflow import Pipeline
from tinyflow.exceptions import NotAnOperation


def test_pipeline_exceptions():
    p = Pipeline()
    with pytest.raises(NotAnOperation):
        p | None
