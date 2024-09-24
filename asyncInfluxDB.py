# -*- coding: utf-8 -*-
"""
Created on 2024/9/24 14:18
@file: asyncInfluxDB.py
@author: Jerry
"""

import time
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from datetime import datetime, timezone
from RookieUtil.RDateUtil import timestamp_utc_datetime
from RookieUtil.RLoggerConfig import logger
import traceback


class InfluxDB:
    def __init__(self, _host, _port, _user_name=None, _password=None):
        self.client = InfluxDBClientAsync('http://{}:{}'.format(_host, _port), token)

    async def set_documents(self, _file_name, _index):
        pass

if __name__ == '__main__':
    token = '6ak0hd8AcIE7Z9v6K9ZgbeDEgvyYAyLH8PrpKmPyZJtYRk9JkEl7OiqIsCMxj2B1gfKQ96QEn-EEkFcLsFkXRg=='
    client = InfluxDB('localhost', 8096)
