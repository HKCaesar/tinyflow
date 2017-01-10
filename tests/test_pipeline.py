"""Tests for ``tinyflow.pipeline``."""


import pytest

from tinyflow.errors import NotATransform
from tinyflow.pipeline import Pipeline


def test_pipeline_exceptions():
    p = Pipeline()
    with pytest.raises(NotATransform):
        p | None
