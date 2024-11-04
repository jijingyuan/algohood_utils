# -*- coding: utf-8 -*-
"""
@Create: 2024/10/24 10:55
@File: asyncZmqUtil.py
@Author: Jingyuan
"""

import ujson as json
import zmq.asyncio

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


class AsyncPushZmq:
    def __init__(self, _host, _port):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect('tcp://{}:{}'.format(_host or '*', _port))

    async def push_msg(self, _msg):
        await self.socket.send_string(json.dumps(_msg))


class AsyncPullZmq:
    def __init__(self, _host, _port):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.bind('tcp://{}:{}'.format(_host or 'localhost', _port))

    async def pull_msg(self):
        rsp = await self.socket.recv()
        return json.loads(rsp)
