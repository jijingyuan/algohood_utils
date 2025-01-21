# -*- coding: utf-8 -*-
"""
@Create on  2024/9/27 6:59
@file: asyncRedisClient.py
@author: Jerry
"""
import asyncio

import redis.asyncio as redis

from algoUtils.loggerUtil import generate_logger

logger = generate_logger(level='INFO')


class AsyncRedisClient:
    def __init__(self, _host, _port):
        self.pool = redis.ConnectionPool.from_url("redis://{}:{}".format(_host, _port))

    async def flush_db(self, _db) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.flushdb()

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def get_db_keys(self, _db) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            keys = await redis_client.keys('*')
            return keys or []

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def remove(self, _db, _key) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        key = [_key] if isinstance(_key, str) else _key
        try:
            await redis_client.select(_db)
            await redis_client.delete(*key)

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def add_str(self, _db, _key, _value) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.set(_key, _value)

        except Exception as e:
            logger.error(e)
            return False

    async def get_str(self, _db, _key) -> bytes or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            value = await redis_client.get(_key)
            return value or b''

        except Exception as e:
            logger.error(e)
            return

    async def incr(self, _db, _key, _amount=1) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.incrby(_key, _amount)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def decr(self, _db, _key, _amount=1) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.decrby(_key, _amount)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def get_hash(self, _db, _key, _field) -> bytes or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            value = await redis_client.hget(_key, _field)
            return value or b''

        except Exception as e:
            logger.error(e)
            return

    async def get_hash_all(self, _db, _key) -> dict or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            keys = await redis_client.hgetall(_key)
            return keys or {}

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def add_hash(self, _db, _key, _field_dict: dict) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.hset(_key, mapping=_field_dict)
            return True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def remove_hash(self, _db, _key, _fields) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            fields = [_fields] if isinstance(_fields, str) else _fields
            await redis_client.select(_db)
            await redis_client.hdel(_key, fields)
            return True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def hash_incr(self, _db, _key, _field, _amount=1) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.hincrby(_key, _field, _amount)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def get_ts_batch_by_key(self, _db, _key, _start_ts, _end_ts, _limit=None) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            batch = await ts.range(_key, _start_ts, _end_ts, count=_limit)
            return batch or {}

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def get_last_by_key(self, _db, _key) -> None or tuple:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            rsp = await ts.get(_key)
            return rsp or tuple()

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def get_ts_batch_by_labels_reverse(self, _db, _start_ts, _end_ts, _labels: dict, _limit=None) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        start_ts = _start_ts if isinstance(_start_ts, str) else int(_start_ts * 1000000)
        end_ts = _end_ts if isinstance(_end_ts, str) else int(_end_ts * 1000000)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            filters = [
                '{}={}'.format(k, v) if isinstance(v, str) else '{}=({})'.format(k, ','.join(v))
                for k, v in _labels.items()
            ]
            batch = await ts.mrevrange(start_ts, end_ts, filters=filters, count=_limit)
            return batch or []

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def get_ts_batch_by_labels(self, _db, _start_ts, _end_ts, _labels: dict, _limit=None) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        start_ts = _start_ts if isinstance(_start_ts, str) else int(_start_ts * 1000000)
        end_ts = _end_ts if isinstance(_end_ts, str) else int(_end_ts * 1000000)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            filters = [
                '{}={}'.format(k, v) if isinstance(v, str) else '{}=({})'.format(k, ','.join(v))
                for k, v in _labels.items()
            ]
            batch = await ts.mrange(start_ts, end_ts, filters=filters, count=_limit)
            return batch or []

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def get_last_batch_by_labels(self, _db, _labels) -> None or list:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            filters = [
                '{}={}'.format(k, v) if isinstance(v, str) else '{}=({})'.format(k, ','.join(v))
                for k, v in _labels.items()
            ]
            batch = await ts.mget(filters=filters)
            return batch or []

        except Exception as e:
            logger.error(e)
            return

        finally:
            await redis_client.aclose()

    async def create_ts_key(self, _db, _key, _labels=None, _duplicate_policy='last') -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            await ts.create(_key, labels=_labels, duplicate_policy=_duplicate_policy)
            return True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def alter_ts_key(self, _db, _key, _labels=None, _duplicate_policy='last') -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            await ts.alter(_key, labels=_labels, duplicate_policy=_duplicate_policy)
            return True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def add_ts_point(self, _db, _key, _timestamp, _value) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            await ts.add(_key, _timestamp, _value)
            return True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def add_ts_batch(self, _db, _batch: list) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            rsp = await ts.madd(_batch)
            check = [int(not isinstance(v, int)) for v in rsp]
            return False if sum(check) > 0 else True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def update_labels(self, _db, _key, _labels) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            await ts.alter(_key, labels=_labels)
            return True

        except Exception as e:
            logger.error(e)
            return False

        finally:
            await redis_client.aclose()

    async def add_set(self, _db, _key, _batch) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        batch = [_batch] if isinstance(_batch, str) else _batch
        try:
            await redis_client.select(_db)
            await redis_client.sadd(_key, *batch)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def get_set(self, _db, _key) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            rsp = await redis_client.smembers(_key)
            return rsp or []

        except Exception as e:
            logger.error(e)
            return

    async def pop_set(self, _db, _key, _batch) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        batch = [_batch] if isinstance(_batch, str) else _batch
        try:
            await redis_client.select(_db)
            await redis_client.srem(_key, *batch)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def lrange(self, _db, _key, _start_index=0, _end_index=-1) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            rsp = await redis_client.lrange(_key, _start_index, _end_index)
            return rsp or []

        except Exception as e:
            logger.error(e)
            return

    async def push(self, _db, _key, _batch) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        batch = [_batch] if isinstance(_batch, str) else _batch
        try:
            await redis_client.select(_db)
            await redis_client.rpush(_key, *batch)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def pull_nowait(self, _db, _key, _amount=1) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            rsp = await redis_client.lpop(_key, _amount)
            return rsp or []

        except Exception as e:
            logger.error(e)
            return

    async def pull_block(self, _db, _key, _timeout=0) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            rsp = await redis_client.blpop([_key], _timeout)
            return [rsp[1]]

        except Exception as e:
            logger.error(e)
            return

    async def info(self, _db, _key):
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            return ts.info(_key)

        except Exception as e:
            logger.error(e)
            return

    async def get_set_by_score(self, _db, _key, _min, _max, _limit=None) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            start = None if _limit is None else 0
            rsp = await redis_client.zrangebyscore(_key, _min, _max, start=start, num=_limit)
            return rsp or []

        except Exception as e:
            logger.error(e)
            return


if __name__ == '__main__':
    import time
    loop = asyncio.get_event_loop()

    client = AsyncRedisClient('localhost', 9001)
    # start = time.time() - 60 * 60
    # end = time.time() - 60 * 30
    # coro = client.get_set_by_score(0, 'aixbt_usdt|crypto|binance_future|ticker', start, end, 1000)
    # rsp = loop.run_until_complete(coro)
    # aa=1
    data_shard = loop.run_until_complete(client.get_hash_all(0, 'data_shard'))
    client_list = [AsyncRedisClient(*k.decode().split(':')) for k in data_shard.keys()]
    # #
    tasks = []
    start_timestamp = '-'
    # end_timestamp = int(time.time() - 60 * 60 * 24 * 1.2)
    end_timestamp = '+'
    labels = {'exchange': 'binance_future', 'field': 'close', 'pair': ['xrp_usdt']}
    # coro = client.get_ts_batch_by_labels_reverse(0, start_timestamp, end_timestamp, labels, 1000)
    # rsp = loop.run_until_complete(coro)
    t1 = time.time()
    for client in client_list:
        tasks.append(client.get_ts_batch_by_labels(0, start_timestamp, end_timestamp, labels, 20000))
        # tasks.append(
        #     client.get_ts_batch_by_key(0, 'btc_usdt|binance_future|trade|close', start_timestamp, end_timestamp, 1000)
        # )
        # tasks.append(client.get_last_by_key(0, 'btc_usdt|binance_future|trade|close'))

    rsp = loop.run_until_complete(asyncio.gather(*tasks))
    print(time.time() - t1)
    aa=1
    # labels = {'exchange': 'binance_future'}
    # start_timestamp = int((time.time() - 60 * 60 * 12) * 1000000)
    # end_timestamp = int((time.time()) * 1000000)
    # coro = client.get_ts_batch_by_labels(0, start_timestamp, end_timestamp, labels, 1000)
    # coro = client.get_ts_batch_by_labels(0, '-', '+', labels, 1000)

    # key = 'btc_usdt|binance_future|trade|timestamp'
    # coro = client.get_ts_batch_by_key(0, key, start_timestamp, end_timestamp, 1000)
    # for _ in range(10):
    #     coro = client.push(0, 'test', [random.random()])
    #     loop.run_until_complete(coro)

    # coro = client.pull_nowait(0, 'test', 1)
    # coro = client.lrange(0, 'test')
    # rsp = loop.run_until_complete(coro)

    # pairs = ['eth_usdt', 'btc_usdt']
    # fields = ['close', 'amount', 'timestamp', 'action']
    # for pair in pairs:
    #     for field in fields:
    #         key = '{}|binance_future|trade|{}'.format(pair, field)
    #         labels = {'pair': pair, 'exchange': 'binance_future', 'data_type': 'trade', 'field': field}
    #         coro = client.update_labels(0, key, labels)
    #         print(loop.run_until_complete(coro))
