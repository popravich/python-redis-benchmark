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
    pytest.param(HiredisParser, marks=pytest.mark.hiredis),
    pytest.param(PythonParser, marks=pytest.mark.pyreader),
])
def redispy(request):
    pool = redis.ConnectionPool(parser_class=request.param)
    r = redis.StrictRedis(connection_pool=pool)
    assert r.flushdb() is True
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
    pytest.param(HiReader(), marks=pytest.mark.hiredis),
    pytest.param(HiReader(encoding='utf-8'), marks=pytest.mark.hiredis),
    pytest.param(PyReader(), marks=pytest.mark.pyreader),
    pytest.param(PyReader(encoding='utf-8'), marks=pytest.mark.pyreader),
    pytest.param(PythonParserReader(), marks=pytest.mark.pyreader),
    pytest.param(PythonParserReader(encoding='utf-8'),
                 marks=pytest.mark.pyreader),
    pytest.param(HiredisParserReader(), marks=pytest.mark.hiredis),
    pytest.param(HiredisParserReader(encoding='utf-8'),
                 marks=pytest.mark.hiredis),
], ids=[
    'hiredis',
    'hiredis(utf-8)',
    'aioredis-python',
    'aioredis-python(utf-8)',
    'redispy-python',
    'redispy-python(utf-8)',
    'redispy-hiredis',
    'redispy-hiredis(utf-8)',
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
                 marks=pytest.mark.hiredis),
    pytest.param((aredis_py_start, None),
                 marks=pytest.mark.pyreader),
    pytest.param((aioredis_start, aioredis_stop),
                 marks=pytest.mark.hiredis),
    pytest.param((aioredis_py_start, aioredis_stop),
                 marks=pytest.mark.pyreader),
    pytest.param((asyncio_redis_start, asyncio_redis_stop),
                 marks=pytest.mark.hiredis),
    pytest.param((asyncio_redis_py_start, asyncio_redis_stop),
                 marks=pytest.mark.pyreader),
], ids=[
    'aredis[hi]',
    'aredis[py]',
    'aioredis[hi]',
    'aioredis[py]',
    'asyncio_redis[hi]',
    'asyncio_redis[py]',
])
def async_redis(loop, request):
    start, stop = request.param
    client = loop.run_until_complete(start())
    yield client
    if stop:
        loop.run_until_complete(stop(client))


if has_uvloop:
    kw = dict(params=[
        pytest.param(uvloop.new_event_loop, marks=pytest.mark.uvloop),
        pytest.param(asyncio.new_event_loop, marks=pytest.mark.asyncio),
    ], ids=[
        'uvloop',
        'asyncio',
    ])
else:
    kw = dict(params=[
        # pytest.param(uvloop.new_event_loop, marks=pytest.mark.uvloop),
        pytest.param(asyncio.new_event_loop, marks=pytest.mark.asyncio),
    ], ids=[
        # 'uvloop',
        'asyncio',
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


@pytest.fixture(scope='session')
def str_1k():
    return 'A' * 2**10


@pytest.fixture(scope='session')
def str_4k():
    return 'A' * 2**12


@pytest.fixture(scope='session')
def str_16k():
    return 'A' * 2**14


@pytest.fixture(scope='session')
def str_32k():
    return 'A' * 2**15


@pytest.fixture(scope='session')
def bulk_data_1k(str_1k):
    d = str_1k.encode('utf-8')
    return b'$%d\r\n%s\r\n' % (len(d), d)


@pytest.fixture(scope='session')
def bulk_data_4k(str_4k):
    d = str_4k.encode('utf-8')
    return b'$%d\r\n%s\r\n' % (len(d), d)


@pytest.fixture(scope='session')
def bulk_data_16k(str_16k):
    d = str_16k.encode('utf-8')
    return b'$%d\r\n%s\r\n' % (len(d), d)


@pytest.fixture(scope='session')
def bulk_data_32k(str_32k):
    d = str_32k.encode('utf-8')
    return b'$%d\r\n%s\r\n' % (len(d), d)
