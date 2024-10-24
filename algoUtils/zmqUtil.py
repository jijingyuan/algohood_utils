# -*- coding: utf-8 -*-
"""
@Create: 2024/10/24 10:55
@File: zmqUtil.py
@Author: Jingyuan
"""

import asyncio

import ujson as json
import zmq
import zmq.asyncio

from .loggerUtil import generate_logger

logger = generate_logger()

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class ReqZmq:

    def __init__(self, _port, _host=None):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect('tcp://{}:{}'.format(_host or 'localhost', _port))

    def send_msg(self, _msg):
        try:
            self.socket.send_json(_msg)
            return self.socket.recv_json() or ''

        except Exception as e:
            logger.error(e)


class RepZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    def recv_msg(self):
        return self.socket.recv_json()

    def send_msg(self, _msg):
        try:
            self.socket.send_json(_msg)
            return True

        except Exception as e:
            logger.error(e)
            return False


class RouterZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    def recv_msg(self):
        return self.socket.recv_multipart()

    def send_msg(self, _request_id, _msg):
        try:
            parts = [_request_id, b'', json.dumps(_msg).encode()]
            self.socket.send_multipart(parts)
            return True

        except Exception as e:
            logger.error(e)
            return False


class AsyncRouterZmq:
    def __init__(self, _port, _host=None):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.bind('tcp://{}:{}'.format(_host or '*', _port))

    async def recv_msg(self):
        return await self.socket.recv_multipart()

    async def send_msg(self, _request_id, _msg):
        try:
            parts = [_request_id, b'', json.dumps(_msg).encode()]
            await self.socket.send_multipart(parts)
            return True

        except Exception as e:
            logger.error(e)
            return False
