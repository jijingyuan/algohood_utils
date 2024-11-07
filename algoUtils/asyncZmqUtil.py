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
            await self.socket.send_string(json.dumps(_msg))
            rsp = await self.socket.recv()
            return json.loads(rsp)

        except Exception as e:
            logger.error(e)


class AsyncRouterZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    async def recv_msg(self):
        request_id, _, msg = await self.socket.recv_multipart()
        return request_id, json.loads(msg.decode())

    async def send_msg(self, _request_id, _msg):
        try:
            parts = [_request_id, b'', json.dumps(_msg).encode()]
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
        msg = json.dumps(_msg)
        await self.socket.send_string('{}||{}'.format(_channel, msg))


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
        channel, msg = rsp.split(b'||')
        return channel.decode(), json.loads(msg)


class AsyncPushZmq:
    def __init__(self, _host, _port):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    async def push_msg(self, _msg):
        await self.socket.send_string(json.dumps(_msg))


class AsyncPullZmq:
    def __init__(self, _host, _port):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect('tcp://{}:{}'.format(_host or 'localhost', _port))

    async def pull_msg(self):
        rsp = await self.socket.recv()
        return json.loads(rsp)
