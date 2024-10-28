# -*- coding: utf-8 -*-
"""
@Create on  2024/9/27 6:59
@file: asyncRedisClient.py
@author: Jerry
"""
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

    async def get_ts_batch_by_labels(self, _db, _start_ts, _end_ts, _labels: dict, _limit=None) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            ts = redis_client.ts()
            batch = await ts.mrange(
                _start_ts, _end_ts, filters=['{}={}'.format(k, v) for k, v in _labels.items()], count=_limit
            )

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

    async def push(self, _db, _key, _batch: list) -> bool:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            await redis_client.rpush(_key, *_batch)
            return True

        except Exception as e:
            logger.error(e)
            return False
        
    async def pull(self, _db, _key, _amount=1) -> list or None:
        redis_client = redis.Redis(connection_pool=self.pool)
        try:
            await redis_client.select(_db)
            rsp = await redis_client.rpop(_key, _amount)
            return rsp or []

        except Exception as e:
            logger.error(e)
            return


if __name__ == '__main__':
    import asyncio
    import time

    loop = asyncio.get_event_loop()

    client = AsyncRedisClient('localhost', 7002)
    labels = {'exchange': 'binance_future'}
    start_timestamp = int((time.time() - 60 * 60 * 12) * 1000000)
    end_timestamp = int((time.time()) * 1000000)
    coro = client.get_ts_batch_by_labels(0, start_timestamp, end_timestamp, labels, 1000)
    # coro = client.get_ts_batch_by_labels(0, '-', '+', labels, 1000)

    # key = 'btc_usdt|binance_future|trade|timestamp'
    # coro = client.get_ts_batch_by_key(0, key, start_timestamp, end_timestamp, 1000)
    rsp = loop.run_until_complete(coro)

    # pairs = ['eth_usdt', 'btc_usdt']
    # fields = ['close', 'amount', 'timestamp', 'action']
    # for pair in pairs:
    #     for field in fields:
    #         key = '{}|binance_future|trade|{}'.format(pair, field)
    #         labels = {'pair': pair, 'exchange': 'binance_future', 'data_type': 'trade', 'field': field}
    #         coro = client.update_labels(0, key, labels)
    #         print(loop.run_until_complete(coro))

    aa = 1
