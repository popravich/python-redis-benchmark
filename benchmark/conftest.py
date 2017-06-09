import pytest
import redis
import io

from unittest import mock
from hiredis import Reader as HiReader
from aioredis.parser import PyReader
from redis.connection import HiredisParser, PythonParser


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
    HiReader(), HiReader(encoding='utf-8'),
    PyReader(), PyReader(encoding='utf-8'),
    PythonParserReader(), PythonParserReader(encoding='utf-8'),
    HiredisParserReader(), HiredisParserReader(encoding='utf-8'),
], ids=[
    'hiredis', 'hiredis(utf-8)',
    'aioredis-python', 'aioredis-python(utf-8)',
    'redispy-python', 'redispy-python(utf-8)',
    'redispy-hiredis', 'redispy-hiredis(utf-8)'])
def reader(request):
    return request.param
