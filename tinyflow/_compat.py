"""Python 2 support."""


import itertools as it
import sys


if sys.version_info.major == 2:  # pragma: no cover
    map = it.imap
    filter = it.ifilter
else:  # pragma: no cover
    map = map
    filter = filter
