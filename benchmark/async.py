import pytest
import asyncio_redis


def execute(loop, coro_func, *args, **kwargs):
    assert loop.run_until_complete(coro_func(*args, **kwargs)) is not None


@pytest.mark.benchmark(group='async-ping')
def benchmark_ping(benchmark, async_redis, loop):
    """Test the smallest and most simple PING command."""
    benchmark(execute, loop, async_redis.ping)


@pytest.mark.benchmark(group='async-set')
def benchmark_set(benchmark, async_redis, loop):
    """Test SET command with value of 1KiB size."""
    key = 'str:key'
    value = 'S' * 1024
    benchmark(execute, loop, async_redis.set, key, value)


@pytest.mark.benchmark(group="async-get")
def benchmark_get__1k(benchmark, async_redis, loop, _aioredis, str_1k):
    """Test get from Redis single 1KiB string value."""
    loop.run_until_complete(_aioredis.set('str:key', str_1k))
    benchmark(execute, loop, async_redis.get, 'str:key')


@pytest.mark.benchmark(group="async-get")
def benchmark_get__4k(benchmark, async_redis, loop, _aioredis, str_4k):
    """Test get from Redis single 4KiB string value."""
    loop.run_until_complete(_aioredis.set('str:key', str_4k))
    benchmark(execute, loop, async_redis.get, 'str:key')


@pytest.mark.benchmark(group="async-get")
def benchmark_get_16k(benchmark, async_redis, loop, _aioredis, str_16k):
    """Test get from Redis single 16KiB string value."""
    loop.run_until_complete(_aioredis.set('str:key', str_16k))
    benchmark(execute, loop, async_redis.get, 'str:key')


@pytest.mark.benchmark(group="async-get")
def benchmark_get_32k(benchmark, async_redis, loop, _aioredis, str_32k):
    """Test get from Redis single 32KiB string value."""
    loop.run_until_complete(_aioredis.set('str:key', str_32k))
    benchmark(execute, loop, async_redis.get, 'str:key')


@pytest.mark.benchmark(group="async-hgetall")
def benchmark_hgetall_10x1k(benchmark, async_redis, loop, _aioredis, str_1k):
    """Test get from hash few big items."""
    for i in range(10):
        loop.run_until_complete(_aioredis.hset(
            'dict:key', 'f:{:04}'.format(i), str_1k))
    benchmark(execute, loop, async_redis.hgetall, 'dict:key')


@pytest.mark.benchmark(group="async-hgetall")
def benchmark_hgetall_1kx10(benchmark, async_redis, loop, _aioredis, str_1k):
    """Test get from hash many small items."""
    for i in range(len(str_1k)):
        loop.run_until_complete(_aioredis.hset(
            'dict:key', 'f:{:04}'.format(i), 'HelloWorld'))
    benchmark(execute, loop, async_redis.hgetall, 'dict:key')


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange__1k(benchmark, async_redis, loop, _aioredis, str_1k):
    """Test get from sorted set 1k items."""
    # TODO: configure data ranges
    loop.run_until_complete(_aioredis.flushdb())
    for i in range(len(str_1k)):
        loop.run_until_complete(_aioredis.zadd(
            'zset:key', i / 2, 'key:{:04}'.format(i)))
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
    else:
        kw = {'withscores': True}
    benchmark(execute, loop, async_redis.zrange,
              'zset:key', 0, -1, **kw)


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange__4k(benchmark, async_redis, loop, _aioredis, str_4k):
    """Test get from sorted set 4k items."""
    # TODO: configure data ranges
    loop.run_until_complete(_aioredis.flushdb())
    for i in range(len(str_4k)):
        loop.run_until_complete(_aioredis.zadd(
            'zset:key', i / 2, 'key:{:04}'.format(i)))
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
    else:
        kw = {'withscores': True}
    benchmark(execute, loop, async_redis.zrange,
              'zset:key', 0, -1, **kw)


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange_16k(benchmark, async_redis, loop, _aioredis, str_16k):
    """Test get from sorted set 16k items."""
    # TODO: configure data ranges
    loop.run_until_complete(_aioredis.flushdb())
    for i in range(len(str_16k)):
        loop.run_until_complete(_aioredis.zadd(
            'zset:key', i / 2, 'key:{:04}'.format(i)))
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
    else:
        kw = {'withscores': True}
    benchmark(execute, loop, async_redis.zrange,
              'zset:key', 0, -1, **kw)
