import pytest


def feed_and_gets(reader, data):
    reader.feed(data)
    assert reader.gets() is not False


FRACTIONED = b'$%d\r\n%s\r\n' % (
    len(b'\r\ndata\r\n') * 128, b'\r\ndata\r\n' * 128)

MULTI_BULK = (
    b'*6\r\n'
    b'$11\r\nHello World\r\n'
    b'+OK\r\n'
    b'-Error\r\n'
    b':1234567\r\n'
    b'$-1\r\n'
    b'*-1\r\n'
)


@pytest.mark.simple_string
@pytest.mark.benchmark(group='simple-string')
def benchmark_parser_simple_string(benchmark, reader):
    benchmark(feed_and_gets, reader, b'+OK\r\n')


@pytest.mark.simple_error
@pytest.mark.benchmark(group='simple-error')
def benchmark_parser_simple_error(benchmark, reader):
    benchmark(feed_and_gets, reader, b'-Error\r\n')


@pytest.mark.bulk_string_1k
@pytest.mark.benchmark(group='bulk-string-fractioned')
def benchmark_parser_bulk_string_fractioned(benchmark, reader):
    benchmark(feed_and_gets, reader, FRACTIONED)


@pytest.mark.bulk_string_1k
@pytest.mark.benchmark(group='bulk-string-1K')
def benchmark_parser_bulk_string_1K(benchmark, reader, bulk_data_1k):
    benchmark(feed_and_gets, reader, bulk_data_1k)


@pytest.mark.bulk_string_4k
@pytest.mark.benchmark(group='bulk-string-4K')
def benchmark_parser_bulk_string_4K(benchmark, reader, bulk_data_4k):
    benchmark(feed_and_gets, reader, bulk_data_4k)


@pytest.mark.bulk_string_16k
@pytest.mark.benchmark(group='bulk-string-16K')
def benchmark_parser_bulk_string_16K(benchmark, reader, bulk_data_16k):
    benchmark(feed_and_gets, reader, bulk_data_16k)


@pytest.mark.bulk_string_32k
@pytest.mark.benchmark(group='bulk-string-32K')
def benchmark_parser_bulk_string_32K(benchmark, reader, bulk_data_32k):
    benchmark(feed_and_gets, reader, bulk_data_32k)


@pytest.mark.multi_bulk
@pytest.mark.benchmark(group='multi-bulk')
def benchmark_parser_multi_bulk(benchmark, reader):
    benchmark(feed_and_gets, reader, MULTI_BULK)


@pytest.mark.multi_bulk
@pytest.mark.benchmark(group='multi-bulk-4K')
def benchmark_parser_multi_bulk_4k(benchmark, reader, bulk_data_1k):
    multi_bulk_4k = b'*4\r\n' + bulk_data_1k
    benchmark(feed_and_gets, reader, multi_bulk_4k)
