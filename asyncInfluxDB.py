# -*- coding: utf-8 -*-
"""
Created on 2024/9/24 14:18
@file: asyncInfluxDB.py
@author: Jerry
"""
import traceback

from algohood_utils.dateUtil import timestamp_utc_datetime_str, timestamp_utc_datetime
from algohood_utils.loggerUtil import generate_logger
from influxdb_client import InfluxDBClient
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

logger = generate_logger(level='INFO')


class InfluxDB:
    def __init__(self, _host, _port, _token, _org):
        self.url = 'http://{}:{}'.format(_host, _port)
        self.token = _token
        self.org = _org

    def get_buckets(self) -> None or list:
        rsp = None
        try:
            influx_client = InfluxDBClient(self.url, self.token, org=self.org, timeout=None)
            buckets = influx_client.buckets_api().find_buckets()
            return [v.name for v in buckets.buckets if v.name not in ['_tasks', '_monitoring']]

        except Exception as e:
            logger.error(e)

        return rsp

    def set_buckets(self, _bucket_name) -> bool:
        try:
            influx_client = InfluxDBClient(self.url, self.token, org=self.org, timeout=None)
            influx_client.buckets_api().create_bucket(bucket_name=_bucket_name)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def set_documents(self, _bucket, _documents) -> bool:
        try:
            async with InfluxDBClientAsync(self.url, self.token, self.org, timeout=None) as influx_client:
                await influx_client.write_api().write(_bucket, record=_documents)
            return True

        except Exception as e:
            logger.error(e)
            return False

    async def get_documents(
            self, _bucket, _start_timestamp, _end_timestamp, _measurement=None, _limit=None, _sort=None, _tags=None
    ) -> None or []:
        response = None
        start_str = timestamp_utc_datetime_str(_start_timestamp)
        end_str = timestamp_utc_datetime_str(_end_timestamp)
        try:
            async with InfluxDBClientAsync(self.url, self.token, self.org, timeout=None) as influx_client:
                query = f'''
                    from(bucket: "{_bucket}")
                    |> range(start: {start_str}, stop: {end_str})
                    '''
                if _tags:
                    for key, value in _tags.items():
                        query += f'|> filter(fn: (r) => r["{key}"] == "{value}")'

                if _measurement:
                    query += f'|> filter(fn: (r) => r._measurement == "{_measurement}")'
                if _limit:
                    query += f'|> limit(n: {_limit})'
                if _sort:
                    query += f'|> sort(columns: ["{_sort}"], desc: true)'
                else:
                    query += f'|> sort(columns: ["_time"], desc: true)'

                query += f'|> keep(columns: ["_field", "_value"])'
                table_list = await influx_client.query_api().query(query)
                response = []
                for tmp in zip(*[v.records for v in table_list]):
                    response.append({v.values['_field']: v.values['_value'] for v in tmp})

        except Exception as e:
            logger.error(traceback.format_exc())

        finally:
            return response

    async def remove_documents_by_filter(self, _bucket, _measurement, _start_timestamp, _end_timestamp) -> bool:
        start_datetime = timestamp_utc_datetime(_start_timestamp)
        end_datetime = timestamp_utc_datetime(_end_timestamp)
        try:
            async with InfluxDBClientAsync(self.url, self.token, self.org, timeout=None) as influx_client:
                await influx_client.delete_api().delete(start_datetime, end_datetime, '', _bucket)
            return True

        except Exception as e:
            logger.error(e)
            return False


if __name__ == '__main__':
    import asyncio
    import time

    loop = asyncio.get_event_loop()
    token = 'G1oUiAo_2FnthaDn4OWSFah1zACmYv84bkp5InbggnXMaDbVp2CO1h0UDbeYRutpwdg8r_Gm7a1hN4SBiU_4Fg=='
    org = 'AlgohoodBackend'
    client = InfluxDB('localhost', 8096, token, org)

    bucket = 'Data'
    measurement = 'trade'
    start_timestamp = 0
    end_timestamp = int(time.time())
    limit = 10000

    coro = client.get_documents(bucket, start_timestamp, end_timestamp, measurement, limit)
    rsp = loop.run_until_complete(coro)

    # measurement = 'test'
    # bucket = 'test'
    # docs = [
    #     {'measurement': measurement, 'fields': {'aaa': 1, 'sss': 2}, 'time': timestamp_utc_datetime(time.time())},
    #     # {'measurement': measurement, 'fields': {'aaa': 3, 'sss': 4}, 'time': int(time.time() * 10000000)},
    # ]
    # #
    # coro = client.set_documents(bucket, docs)
    # loop.run_until_complete(coro)
    # client.set_buckets('test')
    # print(client.get_buckets())

    # coro = client.remove_documents(bucket, measurement, start_utc_str, end_utc_str)
    # loop.run_until_complete(coro)

    aa = 1