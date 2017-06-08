import pytest


@pytest.mark.benchmark(group="redispy-ping")
def benchmark_ping(benchmark, redispy):
    benchmark(redispy.ping)


@pytest.mark.benchmark(group="redispy-set")
@pytest.mark.parametrize('str_size', [10, 2**8, 2**10, 2**11])
def benchmark_set(benchmark, redispy, str_size):
    value = 'S' * str_size
    benchmark(redispy.set, 'str:key', value)


@pytest.mark.benchmark(group="redispy-get")
@pytest.mark.parametrize('str_size', [10, 2**8, 2**10, 2**11])
def benchmark_get(benchmark, redispy, str_size):
    value = 'S' * str_size
    redispy.set('str:key', value)
    benchmark(redispy.get, 'str:key')


@pytest.mark.benchmark(group="redispy-lpush")
@pytest.mark.parametrize('str_size', [10, 2**8, 2**10, 2**11])
@pytest.mark.parametrize('data_size', [1, 10, 100, 1000])
def benchmark_lpush(benchmark, redispy, str_size, data_size):
    value = ['S' * str_size] * data_size
    redispy.delete('lpush')
    benchmark(redispy.lpush, 'lpush', value)
