import argparse
import asyncio
import uvloop
import time

import aioredis
import aredis
import asyncio_redis
import redis
from asyncio_redis import protocol

from functools import wraps, partial
from timeit import timeit


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('-n', type=int, metavar="TOTAL", default=10**5,
                    help="Total number of requests (default %(default)s)")
    ap.add_argument('-P', type=int, metavar="NUM", default=1,
                    help="Pipeline NUM requests. Default 1 (no pipeline).")
    ap.add_argument('-s', type=int, metavar="SIZE", default=2,
                    help=("Data size of SET/GET value in bytes"
                          "(default %(default)s)"))
    ap.add_argument('-R', type=int, metavar="REPEAT", default=1,
                    help="Repeat each test REPEAT times (default %(default)s)."
                    )
    ap.add_argument('--no-uvloop', default=False, action='store_true',
                    help="Run using pure asyncio, not uvloop")
    return ap.parse_args()


# scafolding

def measure(func, ops, number=1):
    total = timeit(func, number=number)
    dur = total / options.R
    return {
        'duration': round(dur, 2),
        'rate': round(ops/dur, 2),
        }


def timer(func):

    @wraps(func)
    def inner(conn, options):
        fun = partial(func, conn=conn,
                      num=options.n,
                      pipeline_size=options.P,
                      data_size=options.s)
        rv = measure(fun, options.n, options.R)
        rv['test'] = func.__name__
        rv['lib'] = conn.__module__.split('.', 1)[0]
        return rv
    return inner


def atimer(corofunc):

    @wraps(corofunc)
    def inner(conn, options):
        loop = asyncio.get_event_loop()
        coro = partial(corofunc, conn=conn,
                       num=options.n,
                       pipeline_size=options.P,
                       data_size=options.s)

        def fun():
            return loop.run_until_complete(coro())
        rv = measure(fun, options.n, options.R)
        rv['test'] = corofunc.__name__[2:]
        rv['lib'] = conn.__module__.split('.', 1)[0]
        return rv
    return inner


# redis-py tests

@timer
def set_str(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    format_str = '{:0<%d}' % data_size
    set_data = format_str.format('a')
    for i in range(num):
        conn.set('set_str:%d' % i, set_data)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def get_str(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    for i in range(num):
        conn.get('set_str:%d' % i)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()

# async tests


@atimer
async def a_set_str(conn, num, pipeline_size, data_size):
    format_str = '{:0<%d}' % data_size
    set_data = format_str.format('a')
    for i in range(num):
        await conn.set('set_str:%d' % i, set_data)


@atimer
async def a_get_str(conn, num, pipeline_size, data_size):

    for i in range(num):
        await conn.get('set_str:%d' % i)


if __name__ == '__main__':
    options = parse_args()

    def echo(rv):
        print("{lib:<20} | {test:10} | {duration:10.2f} | {rate:10.2f}"
              .format(**rv))

    print("{lib:<20} | {test:10} | {duration:10} | {rate:10}".format(
        lib='lib', test='test', duration='duration', rate='rate'))

    if not options.no_uvloop:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    r = redis.StrictRedis()
    r.flushall()

    start = time.time()

    echo(set_str(conn=r, options=options))
    echo(get_str(conn=r, options=options))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    conn = loop.run_until_complete(
        aioredis.create_redis(('localhost', 6379)))
    echo(a_set_str(conn, options))
    echo(a_get_str(conn, options))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    conn = aredis.StrictRedis(host='localhost', port=6379)
    echo(a_set_str(conn, options))
    echo(a_get_str(conn, options))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    conn = loop.run_until_complete(
        asyncio_redis.Connection.create(
            host='localhost', port=6379,
            protocol_class=protocol.HiRedisProtocol
            # protocol_class=protocol.RedisProtocol
            ))
    echo(a_set_str(conn, options))
    echo(a_get_str(conn, options))
    print("Finished in {}s".format(int(time.time() - start)))
