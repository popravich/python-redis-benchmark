Python Redis clients benchmarks
-------------------------------

Run, either::

    $ pip install -r requirements.txt
    $ py.test

or::

    $ vagga run

Tested libraries
~~~~~~~~~~~~~~~~

* `aioredis`_ — async mode (hiredis/python parser), parsers implementation;

* `aredis`_ — async mode (hiredis/python parser);

* `asyncio-redis`_ — async mode (hiredis/python parser);

* `hiredis`_ — parser;

* `redis-py`_ — parsers implementation;


.. _aioredis: https://github.com/aio-libs/aioredis
.. _aredis: https://github.com/NoneGG/aredis
.. _asyncio-redis: https://github.com/jonathanslenders/asyncio-redis
.. _hiredis: https://github.com/redis/hiredis-py
.. _redis-py: https://github.com/andymccurdy/redis-py

Results
~~~~~~~

Test environment

:CPU: Intel(R) Core(TM) i5-4460  CPU @ 3.20GHz

:Cores: 4

:RAM: 16GiB

:Python: CPython 3.6.3

:Redis: Redis server v=4.0.1 sha=00000000:0 malloc=jemalloc-3.6.0 bits=64 build=6f05deda2aff122a
