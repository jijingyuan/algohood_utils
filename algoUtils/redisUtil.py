# -*- coding: utf-8 -*-
"""
@Create on  2024/9/29 8:12
@file: redisClient.py
@author: Jerry
"""
import time

import redis
from algoUtils.loggerUtil import generate_logger

logger = generate_logger(level='INFO')


class RedisClient:
    def __init__(self, _host, _port):
        self.client = redis.Redis.from_url("redis://{}:{}".format(_host, _port))

    def flush_db(self, _db) -> bool:
        try:
            self.client.select(_db)
            self.client.flushdb()
            return True

        except Exception as e:
            logger.error(e)
            return False

    def get_db_keys(self, _db) -> list or None:
        try:
            self.client.select(_db)
            keys = self.client.keys()
            return keys or []

        except Exception as e:
            logger.error(e)
            return

    def remove(self, _db, _key) -> bool:
        try:
            key = [_key] if isinstance(_key, str) else _key
            self.client.select(_db)
            self.client.delete(*key)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def add_str(self, _db, _key, _value) -> bool:
        try:
            self.client.select(_db)
            self.client.set(_key, _value)

        except Exception as e:
            logger.error(e)
            return False

    def get_str(self, _db, _key) -> str or None:
        try:
            self.client.select(_db)
            value = self.client.get(_key)
            return value or ''

        except Exception as e:
            logger.error(e)
            return

    def incr(self, _db, _key, _amount=1) -> bool:
        try:
            self.client.select(_db)
            self.client.incrby(_key, _amount)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def decr(self, _db, _key, _amount=1) -> bool:
        try:
            self.client.select(_db)
            self.client.decrby(_key, _amount)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def get_hash(self, _db, _key, _field) -> str or None:
        try:
            self.client.select(_db)
            value = self.client.hget(_key, _field)
            return value or ''

        except Exception as e:
            logger.error(e)
            return

    def get_hash_all(self, _db, _key) -> dict or None:
        try:
            self.client.select(_db)
            keys = self.client.hgetall(_key)
            return keys or {}

        except Exception as e:
            logger.error(e)
            return

    def add_hash(self, _db, _key, _field_dict: dict) -> bool:
        try:
            self.client.select(_db)
            self.client.hset(_key, mapping=_field_dict)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def get_ts_batch_by_key(self, _db, _key, _start_ts, _end_ts, _limit=None) -> list or None:
        start_ts = _start_ts if isinstance(_start_ts, str) else int(_start_ts * 1000000)
        end_ts = _end_ts if isinstance(_end_ts, str) else int(_end_ts * 1000000)
        try:
            self.client.select(_db)
            ts = self.client.ts()
            batch = ts.range(_key, start_ts, end_ts, count=_limit)
            return batch or {}

        except Exception as e:
            logger.error(e)
            return

    def get_last_by_key(self, _db, _key) -> None or tuple:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            rsp = ts.get(_key)
            return rsp or tuple()

        except Exception as e:
            logger.error(e)
            return

    def get_ts_batch_by_labels(self, _db, _start_ts, _end_ts, _labels: dict, _limit=None) -> list or None:
        start_ts = _start_ts if isinstance(_start_ts, str) else int(_start_ts * 1000000)
        end_ts = _end_ts if isinstance(_end_ts, str) else int(_end_ts * 1000000)
        try:
            self.client.select(_db)
            ts = self.client.ts()
            batch = ts.mrange(
                start_ts, end_ts, filters=['{}={}'.format(k, v) for k, v in _labels.items()], count=_limit
            )
            return batch or []

        except Exception as e:
            logger.error(e)
            return

    def get_last_batch_by_labels(self, _db, _labels):
        try:
            self.client.select(_db)
            ts = self.client.ts()
            batch = ts.mget(filters=['{}={}'.format(k, v) for k, v in _labels.items()])
            return batch or []

        except Exception as e:
            logger.error(e)
            return

    def create_ts_key(self, _db, _key, _labels=None, _duplicate_policy='last') -> bool:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            ts.create(_key, labels=_labels, duplicate_policy=_duplicate_policy)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def add_ts_label(self, _db, _key, _labels, _duplicate_policy='last') -> bool:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            ts.alter(_key, labels=_labels, duplicate_policy=_duplicate_policy)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def add_ts_point(self, _db, _key, _timestamp, _value) -> bool:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            ts.add(_key, _timestamp, _value)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def add_ts_batch(self, _db, _batch: list) -> bool:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            rsp = ts.madd(_batch)
            check = [int(not isinstance(v, int)) for v in rsp]
            return False if sum(check) > 0 else True

        except Exception as e:
            logger.error(e)
            return False

    def update_labels(self, _db, _key, _labels) -> bool:
        try:
            self.client.select(_db)
            ts = self.client.ts()
            ts.alter(_key, labels=_labels)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def push(self, _db, _key, _batch: list) -> bool:
        try:
            self.client.select(_db)
            self.client.rpush(_key, *_batch)
            return True

        except Exception as e:
            logger.error(e)
            return False

    def pull(self, _db, _key) -> bytes or None:
        try:
            self.client.select(_db)
            rsp = self.client.rpop(_key)
            return rsp if rsp else b''

        except Exception as e:
            logger.error(e)
            return

    def get_info(self, _db, _key):
        try:
            self.client.select(_db)
            ts = self.client.ts()
            return ts.info(_key)

        except Exception as e:
            logger.error(e)
            return


if __name__ == '__main__':
    from concurrent.futures import ThreadPoolExecutor, wait

    pool = ThreadPoolExecutor(max_workers=50)
    client = RedisClient('localhost', 9001)
    data_shard = client.get_hash_all(0, 'data_shard')
    client_list = [RedisClient(*k.decode().split(':')) for k in data_shard.keys()]

    tasks = []
    start_timestamp = '-'
    # start_timestamp = 17275801981
    end_timestamp = '+'
    labels = {'pair': 'btc_usdt', 'exchange': 'binance_future'}
    t1 = time.time()
    for client in client_list:
        task = pool.submit(client.get_ts_batch_by_labels, 0, start_timestamp, end_timestamp, labels, 100000)
        tasks.append(task)

    wait(tasks)
    # rsp = [v.result() for v in tasks]
    print(time.time() - t1)

    # create ts key
    # client.create_ts_key(10, 'test')

    # client = RedisClient('localhost', 2001)
    # keys = client.get_db_keys(1)
    # rsp = client.get_ts_batch_by_labels(1, '-', '+', {'pair': 'btc_usdt'})
    # labels = []
    # for key in keys:
    #     pair, exchange, data_type, index_type = key.decode().split('|')
    #     client.add_ts_label(
    #         1, key, {'pair': pair, 'exchange': exchange, 'data_type': data_type, 'index_type': index_type}
    #     )
    #     print('{} finished'.format(key))
    # rsp = client.get_info(1, 'btc_usdt|binance_future|trade|max')
    # rsp = client.get_ts_batch_by_key(1, 'btc_usdt|binance_future|trade|max', '-', '+')
    aa = 1
