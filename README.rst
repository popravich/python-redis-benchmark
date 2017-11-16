Python Redis clients benchmarks
===============================

Run, either::

    $ pip install -r requirements.txt
    $ py.test

or, alternatively, run in container::

    $ vagga run

Tested libraries
----------------

* `aioredis`_ — async mode (hiredis/python parser), parsers implementation;

* `aredis`_ — async mode (hiredis/python parser);

* `asyncio-redis`_ — async mode (hiredis/python parser);

* `redis-py`_ — sync mode (hiredis/python parser), parsers implementation;

* `hiredis`_ — parser;


.. _aioredis: https://github.com/aio-libs/aioredis
.. _aredis: https://github.com/NoneGG/aredis
.. _asyncio-redis: https://github.com/jonathanslenders/asyncio-redis
.. _hiredis: https://github.com/redis/hiredis-py
.. _redis-py: https://github.com/andymccurdy/redis-py

Tests and Results
-----------------

Async ``ping``

Async ``get``

Async ``set``

Async ``hgetall``

Async ``zrange``

Async ``lrange``

Sync ``ping``

Sync ``get``

Sync ``set``

Sync ``hgetall``

Sync ``zrange``

Sync ``lrange``

Parse bulk string reply

Parse multi-bulk reply

Parse error reply

Test environment
~~~~~~~~~~~~~~~~

:CPU: Intel(R) Core(TM) i5-4460  CPU @ 3.20GHz

:Cores: 4

:RAM: 16GiB

:Python: CPython 3.6.3

:Redis: Redis server v=4.0.1 sha=00000000:0 malloc=jemalloc-3.6.0 bits=64 build=6f05deda2aff122a
