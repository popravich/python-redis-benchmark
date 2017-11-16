import pytest
import asyncio_redis
import aredis


def execute(loop, coro_func, *args, **kwargs):
    assert loop.run_until_complete(coro_func(*args, **kwargs)) is not None


@pytest.mark.benchmark(group='async-ping')
def benchmark_ping(benchmark, async_redis, loop):
    """Test the smallest and most simple PING command."""
    benchmark(execute, loop, async_redis.ping)


@pytest.mark.benchmark(group='async-set')
def benchmark_set(benchmark, async_redis, loop, key_set, value_set):
    """Test SET command with value of 1KiB size."""
    # XXX: aredis encodes data with latin-1 encoding,
    #       so we convert str to bytes
    if isinstance(async_redis, aredis.StrictRedis):
        value_set = value_set.encode('utf-8')
    benchmark(execute, loop, async_redis.set, key_set, value_set)


@pytest.mark.benchmark(group="async-get")
def benchmark_get(benchmark, async_redis, loop, key_get):
    """Test get from Redis single 1KiB string value."""
    benchmark(execute, loop, async_redis.get, key_get)


@pytest.mark.benchmark(group="async-hgetall")
def benchmark_hgetall(benchmark, async_redis, loop, key_hgetall):
    """Test get from hash few big items."""
    benchmark(execute, loop, async_redis.hgetall, key_hgetall)


@pytest.mark.benchmark(group="async-lrange")
def benchmark_lrange(benchmark, async_redis, loop, key_lrange):
    benchmark(execute, loop, async_redis.lrange, key_lrange, 0, -1)


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange(benchmark, async_redis, loop, key_zrange):
    """Test get from sorted set 1k items."""
    # NOTE: asyncio_redis implies `withscores` parameter
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
    else:
        kw = {'withscores': True}
    benchmark(execute, loop, async_redis.zrange,
              key_zrange, 0, -1, **kw)
