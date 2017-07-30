========
tinyflow
========

Experiments in data flow programming.

.. image:: https://travis-ci.org/geowurster/tinyflow.svg?branch=master
    :target: https://travis-ci.org/geowurster/tinyflow?branch=master

.. image:: https://coveralls.io/repos/geowurster/tinyflow/badge.svg?branch=master
    :target: https://coveralls.io/r/geowurster/tinyflow?branch=master

After some experimentation, Apache Beam's Python SDK got the API right.
Use that instead.


Standard Word Count Example
===========================

Grab the 5 most common words in ``LICENSE.txt``

.. code-block:: python

    from collections import Counter

    from tinyflow.serial import ops, Pipeline


    pipe = Pipeline() \
        | "Split line into words" >> ops.flatmap(lambda x: x.lower().split()) \
        | "Remove empty lines" >> ops.filter(bool) \
        | "Produce the 5 most common words" >> ops.counter(5) \
        | "Sort by frequency desc" >> ops.sort(key=lambda x: x[1], reverse=True)

    with open('LICENSE.txt') as f:
        results = dict(pipe(f))


Using only Python's builtins:

.. code-block:: python

    from collections import Counter
    import itertools as it

    with open('LICENSE.txt') as f:
        lines = (line.lower().split() for line in f)
        words = it.chain.from_iterable(lines)
        count = Counter(words)
        results = dict(count.most_common(10))


Sub-pipelines, Processes, and Threads
=====================================

Pipelines can be treated as operations and/or mapped across elements in a
stream.  This word count example breaks the workflow into multiple pipelines
to process multiple files across multiple threads to ultimately determine the
10 most frequent words.

.. code-block:: python

    from concurrent.futures import ThreadPoolExecutor
    import operator as op

    from tinyflow import ops, MapPipeline, Pipeline


    # Compute word count for a single file
    wordcount = MapPipeline() \
        | ops.cat() \
        | ops.methodcaller('lower') \
        | ops.methodcaller('split') \
        | ops.filter() \
        | ops.flatten() \
        | ops.counter()

    # Summarize stats across all files
    aggregate_wordcount = Pipeline() \
        | ops.flatten() \
        | ops.reduce_by_key(op.iadd, op.itemgetter(0), op.itemgetter(1))

    # Distributes the process across threads, aggregates, sorts, and
    # determines the 10 most frequent words.
    pipeline = Pipeline() \
       | ops.map(wordcount, pool='thread') \
       | aggregate_wordcount \
       | ops.sort(op.itemgetter(1), reverse=True) \
       | ops.take(10)

    # Execute the pipeline and give it access to 4 threads.
    infiles = ('LICENSE.txt' for _ in range(2))
    with ThreadPoolExecutor(4) as threads:
        for item in pipeline(infiles, thread_pool=threads):
            print(item)


Developing
==========

.. code-block:: console

    $ git clone https://github.com/geowurster/tinyflow.git
    $ cd tinyflow
    $ pip install -e .\[all\]
    $ pytest --cov tinyflow --cov-report term-missing


License
=======

See ``LICENSE.txt``


Changelog
=========

See ``CHANGES.md``
