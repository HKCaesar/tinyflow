"""``pytest`` fixtures."""


import pytest

from tinyflow import __license__
from tinyflow import _testing


@pytest.fixture(scope='module')
def wordcount_input():
    return __license__.splitlines()


@pytest.fixture(scope='module')
def wordcount_top5():
    return {'the': 13, 'of': 12, 'or': 11, 'and': 8, 'in': 6}


@pytest.fixture(scope='module')
def square():
    """Returns a function behaving like ``lambda x: x ** 2`` to get around
    ``pickle``'s limitations.
    """
    return _testing.square


@pytest.fixture(scope='module')
def add2():
    """Returns a function behaving like ``lambda a, b: a + b`` to get around
    ``pickle's`` limitations.
    """
    return _testing.add2


@pytest.fixture(scope='module')
def add4():
    """Same as ``add2()`` but with 4 arguments."""
    return _testing.add4
