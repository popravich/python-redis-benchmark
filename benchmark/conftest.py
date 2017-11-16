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


@pytest.fixture(scope='session', params=[
    pytest.param(HiredisParser, marks=pytest.mark.hiredis, id='redis-py[hi]'),
    pytest.param(PythonParser, marks=pytest.mark.pyreader, id='redis-py[py]'),
])
def redispy(request):
    pool = redis.ConnectionPool(parser_class=request.param)
    r = redis.StrictRedis(connection_pool=pool)
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
    pytest.param(HiReader(), marks=pytest.mark.hiredis,
                 id='hiredis'),
    pytest.param(HiReader(encoding='utf-8'), marks=pytest.mark.hiredis,
                 id='hiredis(utf-8)'),
    pytest.param(PyReader(), marks=pytest.mark.pyreader,
                 id='aioredis-python'),
    pytest.param(PyReader(encoding='utf-8'), marks=pytest.mark.pyreader,
                 id='aioredis-python(utf-8)'),
    pytest.param(PythonParserReader(), marks=pytest.mark.pyreader,
                 id='redispy-python'),
    pytest.param(PythonParserReader(encoding='utf-8'),
                 marks=pytest.mark.pyreader,
                 id='redispy-python(utf-8)'),
    pytest.param(HiredisParserReader(), marks=pytest.mark.hiredis,
                 id='redispy-hiredis'),
    pytest.param(HiredisParserReader(encoding='utf-8'),
                 marks=pytest.mark.hiredis,
                 id='redispy-hiredis(utf-8)'),
])
def reader(request):
    return request.param


async def aredis_start():
    client = aredis.StrictRedis.from_url(
        'redis://localhost:6379',
        max_connections=2)
    await client.ping()
    return client


async def aredis_py_start():
    client = aredis.StrictRedis.from_url(
        'redis://localhost:6379',
        max_connections=2,
        parser_class=aredis_conn.PythonParser)
    await client.ping()
    return client


async def aioredis_start():
    client = await aioredis.create_redis_pool(
        ('localhost', 6379),
        maxsize=2)
    await client.ping()
    return client


async def aioredis_py_start():
    client = await aioredis.create_redis_pool(
        ('localhost', 6379),
        maxsize=2, parser=aioredis_parser.PyReader)
    await client.ping()
    return client


async def aioredis_stop(client):
    client.close()
    await client.wait_closed()


async def asyncio_redis_start():
    pool = await asyncio_redis.Pool.create(
        'localhost', 6379, poolsize=2,
        protocol_class=HiRedisProtocol)
    await pool.ping()
    return pool


async def asyncio_redis_py_start():
    pool = await asyncio_redis.Pool.create(
        'localhost', 6379, poolsize=2,
        protocol_class=RedisProtocol)
    await pool.ping()
    return pool


async def asyncio_redis_stop(pool):
    pool.close()


@pytest.fixture(params=[
    pytest.param((aredis_start, None),
                 marks=pytest.mark.hiredis,
                 id='aredis[hi]-------'),
    pytest.param((aredis_py_start, None),
                 marks=pytest.mark.pyreader,
                 id='aredis[py]-------'),
    pytest.param((aioredis_start, aioredis_stop),
                 marks=pytest.mark.hiredis,
                 id='aioredis[hi]-----'),
    pytest.param((aioredis_py_start, aioredis_stop),
                 marks=pytest.mark.pyreader,
                 id='aioredis[py]-----'),
    pytest.param((asyncio_redis_start, asyncio_redis_stop),
                 marks=pytest.mark.hiredis,
                 id='asyncio_redis[hi]'),
    pytest.param((asyncio_redis_py_start, asyncio_redis_stop),
                 marks=pytest.mark.pyreader,
                 id='asyncio_redis[py]'),
])
def async_redis(loop, request):
    start, stop = request.param
    client = loop.run_until_complete(start())
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
def r():
    return redis.StrictRedis()


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
    for i in range(items):
        r.hset(key, 'f:{:05}'.format(i), val)
    return key


@pytest.fixture(scope='session')
def key_zrange(data_size, r):
    key = 'zset:size:{:05}'.format(data_size)
    for i in range(data_size):
        val = 'val:{:05}'.format(i)
        r.zadd(key, i / 2, val)
    return key


@pytest.fixture(scope='session')
def key_lrange(data_size, r):
    size = MAX_SIZE // data_size
    key = 'list:size:{:05}x{:05}'.format(data_size, size)
    val = data_value(size)
    for i in range(data_size):
        r.lpush(key, val)
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
