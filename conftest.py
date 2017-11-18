import pytest
import redis
import io
import asyncio
try:
    import uvloop
except ImportError:
    has_uvloop = False
else:
    has_uvloop = True

import aredis
import aioredis
import asyncio_redis

from unittest import mock
from hiredis import Reader as HiReader
from aioredis.parser import PyReader
from redis.connection import HiredisParser, PythonParser, Encoder
from aredis import connection as aredis_conn
from aioredis import parser as aioredis_parser
from asyncio_redis.protocol import RedisProtocol, HiRedisProtocol


def pytest_addoption(parser):
    parser.addoption('--redis-host', default='localhost',
                     help="Redis server host")
    parser.addoption('--redis-port', default=6379, type=int,
                     help="Redis server port")


@pytest.fixture(scope='session', params=[
    pytest.param(HiredisParser,
                 marks=[pytest.mark.hiredis, pytest.mark.redispy],
                 id='redis-py[hi]'),
    pytest.param(PythonParser,
                 marks=[pytest.mark.pyreader, pytest.mark.redispy],
                 id='redis-py[py]'),
])
def redispy(request):
    host = request.config.getoption('--redis-host')
    port = request.config.getoption('--redis-port')
    pool = redis.ConnectionPool(host=host, port=port,
                                parser_class=request.param)
    r = redis.StrictRedis(host=host, port=port, connection_pool=pool)
    return r


class FakeSocket(io.BytesIO):

    def recv(self, size):
        return self.read(size)

    def recv_into(self, buf):
        return self.readinto(buf)


class PythonParserReader:
    Parser = PythonParser

    def __init__(self, encoding=None):
        self._sock = FakeSocket()
        self._parser = self.Parser(2**17)
        enc = Encoder(encoding, 'strict', encoding is not None)
        self._parser.on_connect(
            mock.Mock(_sock=self._sock,
                      encoder=enc))

    def feed(self, data):
        if not self._sock.tell():
            self._sock.write(data)
        self._sock.seek(0)

    def gets(self):
        return self._parser.read_response()


class HiredisParserReader(PythonParserReader):
    Parser = HiredisParser


@pytest.fixture(params=[
    pytest.param(None, id='bytes'),
    pytest.param('utf-8', id='utf-8'),
])
def reader_encoding(request):
    return request.param


@pytest.fixture(params=[
    pytest.param(HiReader, marks=pytest.mark.hiredis,
                 id='hiredis'),
    pytest.param(PyReader,
                 marks=[pytest.mark.pyreader, pytest.mark.aioredis],
                 id='aioredis[py]'),
    pytest.param(PythonParserReader,
                 marks=[pytest.mark.pyreader, pytest.mark.redispy],
                 id='redispy[py]'),
    pytest.param(HiredisParserReader,
                 marks=[pytest.mark.hiredis, pytest.mark.redispy],
                 id='redispy[hi]'),
])
def reader(request, reader_encoding):
    if reader_encoding:
        return request.param(encoding=reader_encoding)
    return request.param()


async def aredis_start(host, port):
    client = aredis.StrictRedis.from_url(
        'redis://{}:{}'.format(host, port),
        max_connections=2)
    await client.ping()
    return client


async def aredis_py_start(host, port):
    client = aredis.StrictRedis.from_url(
        'redis://{}:{}'.format(host, port),
        max_connections=2,
        parser_class=aredis_conn.PythonParser)
    await client.ping()
    return client


async def aioredis_start(host, port):
    client = await aioredis.create_redis_pool(
        (host, port),
        maxsize=2)
    await client.ping()
    return client


async def aioredis_py_start(host, port):
    client = await aioredis.create_redis_pool(
        (host, port),
        maxsize=2, parser=aioredis_parser.PyReader)
    await client.ping()
    return client


async def aioredis_stop(client):
    client.close()
    await client.wait_closed()


async def asyncio_redis_start(host, port):
    pool = await asyncio_redis.Pool.create(
        host, port, poolsize=2,
        protocol_class=HiRedisProtocol)
    await pool.ping()
    return pool


async def asyncio_redis_py_start(host, port):
    pool = await asyncio_redis.Pool.create(
        host, port, poolsize=2,
        protocol_class=RedisProtocol)
    await pool.ping()
    return pool


async def asyncio_redis_stop(pool):
    pool.close()


@pytest.fixture(params=[
    pytest.param((aredis_start, None),
                 marks=[pytest.mark.hiredis, pytest.mark.aredis],
                 id='aredis[hi]-------'),
    pytest.param((aredis_py_start, None),
                 marks=[pytest.mark.pyreader, pytest.mark.aredis],
                 id='aredis[py]-------'),
    pytest.param((aioredis_start, aioredis_stop),
                 marks=[pytest.mark.hiredis, pytest.mark.aioredis],
                 id='aioredis[hi]-----'),
    pytest.param((aioredis_py_start, aioredis_stop),
                 marks=[pytest.mark.pyreader, pytest.mark.aioredis],
                 id='aioredis[py]-----'),
    pytest.param((asyncio_redis_start, asyncio_redis_stop),
                 marks=[pytest.mark.hiredis, pytest.mark.asyncio_redis],
                 id='asyncio_redis[hi]'),
    pytest.param((asyncio_redis_py_start, asyncio_redis_stop),
                 marks=[pytest.mark.pyreader, pytest.mark.asyncio_redis],
                 id='asyncio_redis[py]'),
])
def async_redis(loop, request):
    start, stop = request.param
    host = request.config.getoption('--redis-host')
    port = request.config.getoption('--redis-port')
    client = loop.run_until_complete(start(host, port))
    yield client
    if stop:
        loop.run_until_complete(stop(client))


if has_uvloop:
    kw = dict(params=[
        pytest.param(uvloop.new_event_loop, marks=pytest.mark.uvloop,
                     id='uvloop-'),
        pytest.param(asyncio.new_event_loop, marks=pytest.mark.asyncio,
                     id='asyncio'),
    ])
else:
    kw = dict(params=[
        pytest.param(asyncio.new_event_loop, marks=pytest.mark.asyncio,
                     id='asyncio'),
    ])


@pytest.fixture(**kw)
def loop(request):
    """Asyncio event loop, either uvloop or asyncio."""
    loop = request.param()
    asyncio.set_event_loop(None)
    yield loop
    loop.stop()
    loop.run_forever()
    loop.close()


@pytest.fixture
def _aioredis(loop):
    r = loop.run_until_complete(aioredis.create_redis(('localhost', 6379)))
    try:
        yield r
    finally:
        r.close()
        loop.run_until_complete(r.wait_closed())


MIN_SIZE = 10

MAX_SIZE = 2**15


@pytest.fixture(scope='session')
def r(request):
    host = request.config.getoption('--redis-host')
    port = request.config.getoption('--redis-port')
    return redis.StrictRedis(host=host, port=port)


@pytest.fixture(
    scope='session',
    params=[MIN_SIZE, 2**8, 2**10, 2**12, 2**14, MAX_SIZE],
    ids=lambda n: str(n).rjust(5, '-')
    )
def data_size(request):
    return request.param


def data_value(size):
    return ''.join(chr(i) for i in range(size))


@pytest.fixture(scope='session')
def key_get(data_size, r):
    key = 'get:size:{:05}'.format(data_size)
    value = data_value(data_size)
    assert r.set(key, value) is True
    return key


@pytest.fixture(scope='session')
def key_hgetall(data_size, r):
    items = data_size
    size = MAX_SIZE // items
    key = 'dict:size:{:05}x{:05}'.format(items, size)
    val = data_value(size)
    p = r.pipeline()
    for i in range(items):
        p.hset(key, 'f:{:05}'.format(i), val)
    p.execute()
    return key


@pytest.fixture(scope='session')
def key_zrange(data_size, r):
    key = 'zset:size:{:05}'.format(data_size)
    p = r.pipeline()
    for i in range(data_size):
        val = 'val:{:05}'.format(i)
        p.zadd(key, i / 2, val)
    p.execute()
    return key


@pytest.fixture(scope='session')
def key_lrange(data_size, r):
    size = MAX_SIZE // data_size
    key = 'list:size:{:05}x{:05}'.format(data_size, size)
    val = data_value(size)
    p = r.pipeline()
    for i in range(data_size):
        p.lpush(key, val)
    p.execute()
    return key


@pytest.fixture(scope='session')
def key_set(data_size):
    return 'set:size:{:05}'.format(data_size)


@pytest.fixture(scope='session')
def value_set(key_set, data_size, r):
    val = data_value(data_size)
    r.set(key_set, val)
    return val


@pytest.fixture(scope='session')
def parse_bulk_str(data_size):
    val = data_value(data_size).encode('utf-8')
    return b'$%d\r\n%s\r\n' % (len(val), val)


@pytest.fixture(scope='session')
def parse_multi_bulk(data_size):
    items = data_size
    item = MAX_SIZE // items

    val = data_value(item).encode('utf-8')
    val = b'$%d\r\n%s\r\n' % (len(val), val)
    return (b'*%d\r\n' % items) + (val * items)
