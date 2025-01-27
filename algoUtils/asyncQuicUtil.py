# -*- coding: utf-8 -*-
"""
@Create: 2024/11/15 18:52
@File: asyncQuicUtil.py
@Author: Jingyuan
"""
import asyncio
import struct
import traceback
from pathlib import Path
from typing import Optional

import ujson as json
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.asyncio.server import serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived, ConnectionTerminated, HandshakeCompleted

from .defUtil import QuicEventBase
from .loggerUtil import generate_logger

logger = generate_logger(level='DEBUG')


class MyProtocol(QuicConnectionProtocol):
    def __init__(self, *args, _event_mgr: QuicEventBase, _is_client: bool, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_mgr = _event_mgr
        self.is_client = _is_client
        self.stop = False
        self.parts = bytearray()
        self.writer: Optional[asyncio.StreamWriter] = None

    async def keep_alive(self):
        while not self.stop:
            try:
                await self.ping()

            except Exception as e:
                logger.error(e)

            finally:
                await asyncio.sleep(10)

    def handle_cache(self):
        while True:
            if len(self.parts) <= 2:
                return

            count = struct.unpack('>H', self.parts[:2])
            end = count[0] + 2
            if len(self.parts) < end:
                return

            self.event_mgr.cache.put_nowait((self._quic.host_cid, self.parts[2: end]))
            self.parts = self.parts[end:]

    async def generate_stream(self):
        _, self.writer = await self.create_stream(True)

    def quic_event_received(self, _event):
        if isinstance(_event, ConnectionTerminated):
            self.event_mgr.connections.pop(self._quic.host_cid, None)
            logger.info('disconnected host id: {}'.format(self._quic.host_cid))
            self.stop = True
            self.event_mgr.on_disconnected(self._quic.host_cid)

        elif isinstance(_event, HandshakeCompleted):
            self.event_mgr.connections[self._quic.host_cid] = self
            logger.info('connected host id: {}'.format(self._quic.host_cid))
            asyncio.get_event_loop().create_task(self.generate_stream())
            self.event_mgr.on_connected(self._quic.host_cid)
            if self.is_client:
                asyncio.get_event_loop().create_task(self.keep_alive())

        elif isinstance(_event, StreamDataReceived):
            self.parts.extend(_event.data)
            self.handle_cache()

    async def send_msg(self, _msg: bytes):
        prefix = struct.pack('>H', len(_msg))
        self.writer.write(prefix + _msg)
        await self.writer.drain()


class ServerMgr:
    def __init__(self, _port, _event_mgr: QuicEventBase):
        self.port = _port
        self.event_mgr = _event_mgr

    async def start(self):
        configuration = QuicConfiguration(is_client=False)
        configuration.load_cert_chain(certfile=Path("cert.pem"), keyfile=Path("key.pem"))

        await serve(
            '0.0.0.0',
            self.port,
            configuration=configuration,
            create_protocol=lambda *args, **kwargs: MyProtocol(
                *args, **kwargs, _event_mgr=self.event_mgr, _is_client=False
            ),
        )
        logger.info('start server')
        while True:
            await self.event_mgr.loop_service()
            await asyncio.sleep(5)


class ClientMgr:
    def __init__(self, _event_mgr: QuicEventBase):
        self.event_mgr = _event_mgr

    async def create_connection(self, _host, _port):
        configuration = QuicConfiguration(is_client=True)
        configuration.verify_mode = False  # 跳过证书验证（仅用于测试）

        while True:
            try:
                async with connect(
                        _host,
                        _port,
                        configuration=configuration,
                        create_protocol=lambda *args, **kwargs: MyProtocol(
                            *args, **kwargs, _event_mgr=self.event_mgr, _is_client=True
                        ),
                ) as protocol:
                    while True:
                        await protocol.wait_closed()
                        break

            except Exception as e:
                logger.error(traceback.format_exc())
                await asyncio.sleep(1)


class DataClientEventMgr(QuicEventBase):
    def __init__(self):
        super().__init__()
        self.__sub_channels = set()

    def on_connected(self, _host_id: bytes):
        if self.__sub_channels:
            asyncio.get_event_loop().create_task(self.subscribe())

    async def subscribe(self, _channels=None):
        if _channels is None:
            add_channels = list(self.__sub_channels)
        else:
            _channels = [_channels] if isinstance(_channels, str) else _channels
            add_channels = [v for v in _channels if v not in self.__sub_channels]

        if add_channels:
            self.__sub_channels.update(add_channels)
            msg = json.dumps({'subject': 'subscribe', 'msg': add_channels}).encode()
            await self.send_all(msg)

    async def unsubscribe(self, _channels):
        _channels = [_channels] if isinstance(_channels, str) else _channels
        remove_channels = [v for v in _channels if v in self.__sub_channels]
        if remove_channels:
            self.__sub_channels.difference_update(remove_channels)
            msg = json.dumps({'subject': 'unsubscribe', 'msg': remove_channels}).encode()
            await self.send_all(msg)


class DataServerEventMgr(QuicEventBase):
    def __init__(self):
        super().__init__()
        self.sub_channels = {}

    def on_disconnected(self, _host_id: bytes):
        try:
            asyncio.get_event_loop().create_task(
                self.update_sub_channels(_host_id, list(self.sub_channels), 'unsub')
            )

        except Exception as e:
            logger.error(e)

    async def update_sub_channels(self, _host_id, _channels, _operation):
        if not isinstance(_channels, list):
            logger.error('{} is not list'.format(_channels))
            return

        async with asyncio.Lock():
            if _operation == 'sub':
                for channel in _channels:
                    self.sub_channels.setdefault(channel, set()).add(_host_id)

            else:
                for channel in _channels:
                    host_ids = self.sub_channels.get(channel, set())
                    if _host_id not in host_ids:
                        continue

                    host_ids.remove(_host_id)
                    if not host_ids:
                        self.sub_channels.pop(channel)

    async def loop_service(self):
        while True:
            host_id, msg = await self.get_data()
            try:
                sub_msg = json.loads(msg.decode())
                if sub_msg['subject'] == 'subscribe':
                    await self.update_sub_channels(host_id, sub_msg['msg'], 'sub')

                elif sub_msg['subject'] == 'unsubscribe':
                    await self.update_sub_channels(host_id, sub_msg['msg'], 'unsub')

                else:
                    logger.error('unknown subject: {}'.format(sub_msg['subject']))

            except Exception as e:
                logger.error(e)

    async def publish(self, _channel, _msg):
        host_ids = self.sub_channels.get(_channel)
        if not host_ids:
            return

        tasks = [self.send_msg(v, _msg) for v in host_ids]
        if tasks:
            await asyncio.gather(*tasks)
