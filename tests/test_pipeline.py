"""Tests for ``tinyflow.pipeline``."""


import pytest

from tinyflow.exceptions import NotAnOperation
from tinyflow.pipeline import Pipeline


def test_pipeline_exceptions():
    p = Pipeline()
    with pytest.raises(NotAnOperation):
        p | None
