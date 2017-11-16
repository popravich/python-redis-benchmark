import pytest


def execute(func, *args, **kwargs):
    assert func(*args, **kwargs) is not None


@pytest.mark.benchmark(group="redispy-ping")
def benchmark_ping(benchmark, redispy):
    benchmark(execute, redispy.ping)


@pytest.mark.benchmark(group="redispy-set")
def benchmark_set(benchmark, redispy, key_set, value_set):
    benchmark(execute, redispy.set, key_set, value_set)


@pytest.mark.benchmark(group="redispy-get")
def benchmark_get(benchmark, redispy, key_get):
    benchmark(execute, redispy.get, key_get)


@pytest.mark.benchmark(group='redispy-hgetall')
def benchmark_hgetall(benchmark, redispy, key_hgetall):
    benchmark(execute, redispy.hgetall, key_hgetall)


@pytest.mark.benchmark(group="redispy-lrange")
def benchmark_lrange(benchmark, redispy, key_lrange):
    benchmark(execute, redispy.lrange, key_lrange, 0, -1)


@pytest.mark.benchmark(group="redispy-zrange")
def benchmark_zrange(benchmark, redispy, key_zrange):
    benchmark(execute, redispy.zrange, key_zrange, 0, -1, withscores=True)
