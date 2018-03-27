import pytest

from aioredis.util import encode_command
from aredis.connection import BaseConnection


@pytest.fixture(params=[
    'string', b'bytess', 10**5, 10 / 3,
])
def data(request):
    return request.param


@pytest.mark.encoder
@pytest.mark.benchmark(group='encoder')
@pytest.mark.parametrize('repeat', [
    1, 10, 100, 1000,
])
def benchmark_aioredis_encoder(benchmark, data, repeat):

    def do(cmd, args):
        val = encode_command(cmd, *args)
        assert isinstance(val, bytearray)
    benchmark(do, 'foo', [data] * repeat)


@pytest.mark.encoder
@pytest.mark.benchmark(group='encoder')
@pytest.mark.parametrize('repeat', [
    1, 10, 100, 1000,
])
def benchmark_aredis_encoder(benchmark, data, repeat):
    conn = BaseConnection()

    def do(cmd, args):
        val = conn.pack_command(cmd, *args)
        assert all(isinstance(x, bytes) for x in val)
    benchmark(do, 'foo', [data] * repeat)
