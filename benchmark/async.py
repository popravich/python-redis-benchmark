import pytest
import asyncio_redis


def execute(loop, coro_func, *args, **kwargs):
    assert loop.run_until_complete(coro_func(*args, **kwargs)) is not None


@pytest.mark.benchmark(group='async-ping')
def benchmark_ping(benchmark, async_redis, loop):
    benchmark(execute, loop, async_redis.ping)


@pytest.mark.benchmark(group='async-set')
def benchmark_set(benchmark, async_redis, loop):
    key = 'str:key'
    value = 'S' * 1024
    benchmark(execute, loop, async_redis.set, key, value)


@pytest.mark.benchmark(group="async-get")
def benchmark_get(benchmark, async_redis, loop, _aioredis):
    value = 'S' * 1024
    loop.run_until_complete(_aioredis.set('str:key', value))
    benchmark(execute, loop, async_redis.get, 'str:key')


@pytest.mark.benchmark(group="async-hgetall")
def benchmark_hgetall(benchmark, async_redis, loop, _aioredis):
    for i in range(10):
        loop.run_until_complete(_aioredis.hset(
            'dict:key', 'field:{}'.format(i), 'some-value'))
    benchmark(execute, loop, async_redis.hgetall, 'dict:key')


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange(benchmark, async_redis, loop, _aioredis):
    for i in range(10):
        loop.run_until_complete(_aioredis.zadd(
            'zset:key', i, 'key:{}'.format(i)))
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
    else:
        kw = {'withscores': True}
    benchmark(execute, loop, async_redis.zrange,
              'zset:key', 0, -1, **kw)
