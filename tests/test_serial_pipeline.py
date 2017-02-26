"""Tests for ``tinyflow.serial.pipeline``."""


import pytest

from tinyflow.exceptions import NotAnOperation
from tinyflow.serial.pipeline import Pipeline


def test_pipeline_exceptions():
    p = Pipeline()
    with pytest.raises(NotAnOperation):
        p | None
