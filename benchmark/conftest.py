import pytest
import redis
import io
import asyncio
import uvloop

import aredis
import aioredis
import asyncio_redis

from unittest import mock
from hiredis import Reader as HiReader
from aioredis.parser import PyReader
from redis.connection import HiredisParser, PythonParser
from aredis import connection as aredis_conn
from aioredis import parser as aioredis_parser
from asyncio_redis.protocol import RedisProtocol, HiRedisProtocol


@pytest.fixture(scope='session', params=[HiredisParser, PythonParser])
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
        self._parser.on_connect(
            mock.Mock(_sock=self._sock,
                      decode_responses=encoding is not None,
                      encoding=encoding))

    def feed(self, data):
        if not self._sock.tell():
            self._sock.write(data)
        self._sock.seek(0)

    def gets(self):
        return self._parser.read_response()


class HiredisParserReader(PythonParserReader):
    Parser = HiredisParser


@pytest.fixture(params=[
    HiReader(),
    HiReader(encoding='utf-8'),
    pytest.mark.pyreader(PyReader()),
    pytest.mark.pyreader(PyReader(encoding='utf-8')),
    pytest.mark.pyreader(PythonParserReader()),
    pytest.mark.pyreader(PythonParserReader(encoding='utf-8')),
    HiredisParserReader(),
    HiredisParserReader(encoding='utf-8'),
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
    (aredis_start, None),
    pytest.mark.pyreader((aredis_py_start, None)),
    (aioredis_start, aioredis_stop),
    pytest.mark.pyreader((aioredis_py_start, aioredis_stop)),
    (asyncio_redis_start, asyncio_redis_stop),
    pytest.mark.pyreader((asyncio_redis_py_start, asyncio_redis_stop)),
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


@pytest.fixture(params=[
    uvloop.new_event_loop,
    asyncio.new_event_loop,
], ids=[
    'uvloop',
    'asyncio',
])
def loop(request):
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
