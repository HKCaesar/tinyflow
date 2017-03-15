"""``pytest`` fixtures."""


import pytest

from tinyflow import __license__


@pytest.fixture(scope='module')
def wordcount_input():
    return __license__.splitlines()


@pytest.fixture(scope='module')
def wordcount_top5():
    return {'the': 13, 'of': 12, 'or': 11, 'and': 8, 'in': 6}
