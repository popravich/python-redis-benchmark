import pytest

from hiredis import Reader as HiReader
from aioredis.parser import PyReader
# from redis.connection import HiredisParser, PythonParser


def feed_and_gets(reader, data):
    reader.feed(data)
    assert reader.gets() is not False


@pytest.mark.benchmark(group='simple-string')
@pytest.mark.parametrize('reader', [
    HiReader(),
    HiReader(encoding='utf-8'),
    PyReader(),
    PyReader(encoding='utf-8'),
], ids='hiredis,hiredis(utf-8),pyreader,pyreader(utf-8)'.split(','))
def benchmark_parser_simple_string(benchmark, reader):
    benchmark(feed_and_gets, reader, b'+OK\r\n')


@pytest.mark.benchmark(group='simple-error')
@pytest.mark.parametrize('reader', [
    HiReader(),
    HiReader(encoding='utf-8'),
    PyReader(),
    PyReader(encoding='utf-8'),
], ids='hiredis,hiredis(utf-8),pyreader,pyreader(utf-8)'.split(','))
def benchmark_parser_simple_error(benchmark, reader):
    benchmark(feed_and_gets, reader, b'-Error\r\n')


def data(s):
    return b'$%d\r\n%s\r\n' % (s, b'A' * s)


BULK_STR_1K = data(2**10)
BULK_STR_4K = data(2**12)
BULK_STR_16K = data(2**14)
BULK_STR_32K = data(2**15)


@pytest.mark.benchmark(group='bulk-string-1K')
@pytest.mark.parametrize('reader', [
    HiReader(),
    HiReader(encoding='utf-8'),
    PyReader(),
    PyReader(encoding='utf-8'),
], ids='hiredis,hiredis(utf-8),pyreader,pyreader(utf-8)'.split(','))
def benchmark_parser_bulk_string_1K(benchmark, reader):
    benchmark(feed_and_gets, reader, BULK_STR_1K)


@pytest.mark.benchmark(group='bulk-string-4K')
@pytest.mark.parametrize('reader', [
    HiReader(),
    HiReader(encoding='utf-8'),
    PyReader(),
    PyReader(encoding='utf-8'),
], ids='hiredis,hiredis(utf-8),pyreader,pyreader(utf-8)'.split(','))
def benchmark_parser_bulk_string_4K(benchmark, reader):
    benchmark(feed_and_gets, reader, BULK_STR_4K)


@pytest.mark.benchmark(group='bulk-string-16K')
@pytest.mark.parametrize('reader', [
    HiReader(),
    HiReader(encoding='utf-8'),
    PyReader(),
    PyReader(encoding='utf-8'),
], ids='hiredis,hiredis(utf-8),pyreader,pyreader(utf-8)'.split(','))
def benchmark_parser_bulk_string_16K(benchmark, reader):
    benchmark(feed_and_gets, reader, BULK_STR_16K)


@pytest.mark.benchmark(group='bulk-string-32K')
@pytest.mark.parametrize('reader', [
    HiReader(),
    HiReader(encoding='utf-8'),
    PyReader(),
    PyReader(encoding='utf-8'),
], ids='hiredis,hiredis(utf-8),pyreader,pyreader(utf-8)'.split(','))
def benchmark_parser_bulk_string_32K(benchmark, reader):
    benchmark(feed_and_gets, reader, BULK_STR_32K)
