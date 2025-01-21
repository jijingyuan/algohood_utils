# -*- coding: utf-8 -*-
"""
@Create: 2024/10/24 10:55
@File: asyncZmqUtil.py
@Author: Jingyuan
"""
import numpy as np
import time
import ujson as json
import zmq.asyncio
from collections import deque

from .loggerUtil import generate_logger

logger = generate_logger()


class AsyncReqZmq:

    def __init__(self, _port, _host=None):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect('tcp://{}:{}'.format(_host or 'localhost', _port))

    async def send_msg(self, _msg):
        try:
            await self.socket.send(_msg)
            rsp = await self.socket.recv()
            return rsp

        except Exception as e:
            logger.error(e)


class AsyncRouterZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    async def recv_msg(self):
        request_id, _, msg = await self.socket.recv_multipart()
        return request_id, msg

    async def send_msg(self, _request_id, _msg):
        try:
            parts = [_request_id, b'', _msg]
            await self.socket.send_multipart(parts)
            return True

        except Exception as e:
            logger.error(e)
            return False


class AsyncPubZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    async def pub_msg(self, _channel, _msg):
        await self.socket.send(b' '.join([_channel, _msg]))


class AsyncSubZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect('tcp://{}:{}'.format(_host or 'localhost', _port))
        self.subscribe_sets = set()

    async def subscribe(self, _channels):
        channels = [_channels] if isinstance(_channels, str) else _channels
        sub_channels = [v for v in channels if v not in self.subscribe_sets]
        for channel in sub_channels:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, channel)
            self.subscribe_sets.add(channel)

    async def unsubscribe(self, _channels):
        channels = [_channels] if isinstance(_channels, str) else _channels
        unsub_channels = [v for v in channels if v in self.subscribe_sets]
        for channel in unsub_channels:
            self.socket.setsockopt_string(zmq.UNSUBSCRIBE, channel)
            self.subscribe_sets.remove(channel)

    async def recv_msg(self):
        rsp = await self.socket.recv()
        channel, msg = rsp.split(b' ', 1)
        return channel, msg
