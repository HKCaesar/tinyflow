#!/usr/bin/env python


"""Setup script for ``tinyflow``."""


import itertools as it
import os
import sys

from setuptools import find_packages
from setuptools import setup


with open('README.rst') as f:
    readme = f.read().strip()


def parse_dunder_line(string):

    """Take a line like:

        "__version__ = '0.0.8'"

    and turn it into a tuple:

        ('__version__', '0.0.8')

    Not very fault tolerant.
    """

    # Split the line and remove outside quotes
    variable, value = (s.strip() for s in string.split('=')[:2])
    value = value[1:-1].strip()
    return variable, value


with open(os.path.join('tinyflow', '__init__.py')) as f:
    dunders = dict(map(
        parse_dunder_line, filter(lambda l: l.strip().startswith('__'), f)))
    version = dunders['__version__']
    author = dunders['__author__']
    email = dunders['__email__']
    source = dunders['__source__']


extras_require = {
    'dev': [
        'pytest>=3',
        'pytest-cov',
        'coveralls',
    ],
}
if sys.version_info.major == 2:
    extras_require['dev'].append('futures')
extras_require['all'] = list(it.chain.from_iterable(extras_require.values()))


setup(
    name='tinyflow',
    author=author,
    author_email=email,
    classifiers=[
        'Topic :: Utilities',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Topic :: Text Processing',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    description="Experimental in-memory data flow pipelines.",
    include_package_data=True,
    extras_require=extras_require,
    keywords='experimental memory data flow',
    license="New BSD",
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    url=source,
    version=version,
    zip_safe=True
)
