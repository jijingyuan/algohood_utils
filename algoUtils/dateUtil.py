# -*- coding: utf-8 -*-
"""
Created on 2024/9/24 9:09
@file: dateUtil.py
@author: Jerry
"""
import calendar
import datetime
import time


def local_datetime_timestamp(_datetime_str):
    t = time.strptime(_datetime_str, '%Y-%m-%d %H:%M:%S')
    return calendar.timegm(t) - 60 * 60 * 8


def date_list_given_start_end(_start_str, _end_str):
    date_list = []
    start_date = datetime.datetime.strptime(_start_str, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(_end_str, '%Y-%m-%d')
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)

    return date_list


def timestamp_local_datetime(_timestamp):
    time_tuple = time.gmtime(int(_timestamp) + 8 * 60 * 60)
    return time.strftime('%Y-%m-%d %H:%M:%S', time_tuple)


def timestamp_utc_datetime_str(_timestamp):
    res = str(_timestamp).split('.')
    time_tuple = time.gmtime(int(res[0]))
    if len(res) > 1:
        return time.strftime('%Y-%m-%dT%H:%M:%S.{}Z'.format(res[1]), time_tuple)
    else:
        return time.strftime('%Y-%m-%dT%H:%M:%SZ', time_tuple)


def timestamp_utc_datetime(_timestamp):
    return datetime.datetime.utcfromtimestamp(_timestamp)


def timestamp_local_datetimestamp(_timestamp):
    extra_time = int(_timestamp * 1000) % 1000
    time_tuple = time.gmtime(int(_timestamp) + 8 * 60 * 60)
    return time.strftime('%Y-%m-%d %H:%M:%S', time_tuple) + 'Z%d' % extra_time
