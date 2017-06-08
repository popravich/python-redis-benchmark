import pytest
import redis
from redis.connection import HiredisParser, PythonParser


@pytest.fixture(scope='session', params=[HiredisParser, PythonParser])
def redispy(request):
    pool = redis.ConnectionPool(parser_class=request.param)
    r = redis.StrictRedis(connection_pool=pool)
    assert r.flushdb() is True
    return r
