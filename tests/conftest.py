"""``pytest`` fixtures."""


from collections import Counter
import itertools as it
import textwrap

import pytest

from tinyflow.pipeline import Pipeline
import tinyflow.transform as t


@pytest.fixture(scope='module')
def text():

    return textwrap.dedent("""
        New BSD License

        Copyright (c) 2017, Kevin D. Wurster
        All rights reserved.

        Redistribution and use in source and binary forms, with or without
        modification, are permitted provided that the following conditions are met:

        * Redistributions of source code must retain the above copyright notice, this
          list of conditions and the following disclaimer.

        * Redistributions in binary form must reproduce the above copyright notice,
          this list of conditions and the following disclaimer in the documentation
          and/or other materials provided with the distribution.

        * The names of tinyflow or its contributors may not be used to endorse or
          promote products derived from this software without specific prior written
          permission.

        THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
        AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
        IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
        DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
        FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
        DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
        SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
        CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
        OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
        OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
        """.strip())


@pytest.fixture(scope='module')
def wordcount_transforms():
    return [
        t.Map(lambda x: x.lower().split()),
        t.Wrap(it.chain.from_iterable),
        t.Wrap(lambda x: Counter(x).most_common(5)),
        t.Sort(key=lambda x: x[1], reverse=True)
    ]
