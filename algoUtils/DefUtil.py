# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 16:58
@File: DefUtil.py
@Author: Jingyuan
"""
import abc
import asyncio
import uuid
from decimal import Decimal
from queue import PriorityQueue
from typing import Optional, List, Dict, AnyStr

from .loggerUtil import generate_logger
from .onlineLoggerUtil import OnlineLogger

logger = generate_logger()


class QuicEventBase:
    def __init__(self):
        self.connections = {}
        self.cache = asyncio.Queue()

    def on_connected(self, _host_id: bytes):
        pass

    def on_disconnected(self, _host_id: bytes):
        pass

    async def get_data(self):
        return await self.cache.get()

    async def send_msg(self, _host_id: bytes, _msg: bytes):
        conn = self.connections.get(_host_id)
        if not conn:
            logger.error('{} is not available'.format(_host_id))
            return

        await conn.send_msg(_msg)

    async def send_all(self, _msg: bytes):
        tasks = [v.send_msg(_msg) for v in self.connections.values()]
        await asyncio.gather(*tasks)

    async def loop_service(self):
        pass


class SignalBase:

    @abc.abstractmethod
    def generate_signals(self, _data: list) -> Optional[List[Dict]]:
        """
        generate signal to execution module
        :param _data: data source
        :return: [{
            'batch_id': str(uuid),
            'symbol': 'btc_usdt|binance_future',
            'action': 'open' or 'close'
            'position': 'long' or 'short'
            **kwargs: other info that you need for analyze
        }, ...]
        """
        return


class TargetBase:

    @abc.abstractmethod
    def generate_targets(self, _data: list) -> Optional[Dict]:
        """
        generate target for signals
        :param _data: data source
        :return: {**kwargs: stats that you need for analyzing}
        """
        return


class OptimizerBase:

    def __init__(self):
        self.__data_mgr = None
        self.__realized_ret = {}
        self.__unrealized_ret = {}

    def update_inits(self, _data_mgr, _realized_ret, _unrealized_ret):
        self.__data_mgr = _data_mgr
        self.__realized_ret = _realized_ret
        self.__unrealized_ret = _unrealized_ret

    def get_realized_ret(self):
        return self.__realized_ret.copy()

    def get_unrealized_ret(self):
        return self.__unrealized_ret.copy()

    def get_backward_data(self, _symbols, _backward_window):
        pass

    @abc.abstractmethod
    def handle_event(self, _event_type, _event) -> Optional[List[AnyStr]]:
        pass


class RiskBase:

    @abc.abstractmethod
    def handle_event(self, _event_type, _event):
        pass


class LiquidityBase:

    @abc.abstractmethod
    def handle_event(self, _event_type, _event):
        pass


class OrderBase:
    ORDER_STATUS = {
        'pending': 0,
        'waiting': 1,
        'triggered': 2,
        'partial_filled': 3,
        'canceling': 4,
        'canceled': 5,
        'error': 5,
        'filled': 5
    }

    SNIFFER_STATUS = {
        'pending': 0,
        'waiting': 1,
        'triggered': 2,
        'canceled': 2,
        'error': 2
    }

    def __init__(self):
        self.__precision_dict = {}
        self.__orders = {}
        self.__sniffers = {}

    @abc.abstractmethod
    def get_trading_amount(self, _symbol):
        pass

    @abc.abstractmethod
    def get_current_timestamp(self):
        pass

    def format_amount(self, _symbol, _amount, _upper=True):
        amount_p = self.__precision_dict[_symbol]['amount']
        factor = 10 ** amount_p
        int_amount = int(_amount * factor)
        bias = 1 if _upper else 0
        return round((int_amount + bias) / factor, amount_p)

    def format_price(self, _symbol, _price, _upper=True):
        amount_p = self.__precision_dict[_symbol]['price']
        factor = 10 ** amount_p
        int_amount = int(_price * factor)
        bias = 1 if _upper else 0
        return round((int_amount + bias) / factor, amount_p)

    @abc.abstractmethod
    async def place_order(
            self,
            _batch_id,
            _symbol,
            _order_type,
            _action,
            _position,
            _amount,
            _feature=None,
            _expire=None,
            _delay=None,
            _condition=None,
            _price=None,
    ) -> str:
        pass

    @abc.abstractmethod
    async def place_target_sniffer(
            self,
            _batch_id,
            _symbol,
            _operator,
            _target_price,
            _expire=None,
            _delay=None
    ) -> str:
        pass

    @abc.abstractmethod
    async def place_trailing_sniffer(
            self,
            _batch_id,
            _symbol,
            _operator,
            _back_pct,
            _smooth=None,
            _expire=None,
            _delay=None
    ) -> str:
        pass

    @abc.abstractmethod
    async def cancel_order(self, _order_id, _delay=None):
        pass

    @abc.abstractmethod
    async def cancel_sniffer(self, _order_id, _delay=None):
        pass

    def _get_order_info(self, _order_id):
        return self.__orders.get(_order_id)

    def _get_sniffer_info(self, _order_id):
        return self.__sniffers.get(_order_id)

    def _set_precision_dict(self, _symbol, _price, _amount):
        self.__precision_dict[_symbol] = {'price': _price, 'amount': _amount}

    def _get_precision_dict(self, _symbol):
        return self.__precision_dict.get(_symbol)

    def _update_round_timestamp(self, _order_id, _send_timestamp=None, _receive_timestamp=None):
        if _send_timestamp is not None:
            self.__orders[_order_id]['send_timestamp'] = _send_timestamp

        if _receive_timestamp is not None:
            self.__orders[_order_id]['receive_timestamp'] = _receive_timestamp

    def _generate_target_sniffer(
            self,
            _batch_id,
            _signal_name,
            _execute_name,
            _symbol,
            _exchange,
            _target_price,
            _operator,
            _smooth=None,
            _expire=None,
            _delay=None
    ):

        order_id = str(uuid.uuid4())
        _, exchange = _symbol.split('|')
        self.__sniffers[order_id] = {
            'order_id': order_id,
            'batch_id': _batch_id,
            'signal_name': _signal_name,
            'execute_name': _execute_name,
            'symbol': _symbol,
            'exchange': _exchange,
            'order_type': 'target',
            'operator': _operator,
            'target_price': _target_price,
            'smooth': _smooth,
            'expire': _expire,
            'delay': _delay,
            'status': 'pending',
            'last_timestamp': None,
            'local_timestamp': None,
        }

        return order_id

    def _generate_trailing_sniffer(
            self,
            _batch_id,
            _signal_name,
            _execute_name,
            _symbol,
            _exchange,
            _operator,
            _back_pct,
            _smooth=None,
            _expire=None,
            _delay=None
    ):

        order_id = str(uuid.uuid4())
        _, exchange = _symbol.split('|')
        self.__sniffers[order_id] = {
            'order_id': order_id,
            'batch_id': _batch_id,
            'signal_name': _signal_name,
            'execute_name': _execute_name,
            'symbol': _symbol,
            'exchange': _exchange,
            'order_type': 'trailing',
            'operator': _operator,
            'back_pct': _back_pct,
            'smooth': _smooth,
            'expire': _expire,
            'delay': _delay,
            'status': 'pending',
            'last_timestamp': None,
            'local_timestamp': None,
        }

        return order_id

    def _generate_order(
            self,
            _batch_id,
            _signal_name,
            _execute_name,
            _symbol,
            _exchange,
            _order_type,
            _action,
            _position,
            _amount,
            _feature=None,
            _expire=None,
            _delay=None,
            _condition=None,
            _price=None,
    ):
        order_id = str(uuid.uuid4())
        self.__orders[order_id] = {
            'order_id': order_id,
            'batch_id': _batch_id,
            'signal_name': _signal_name,
            'execute_name': _execute_name,
            'symbol': _symbol,
            'exchange': _exchange,
            'order_type': _order_type,
            'action': _action,
            'position': _position,
            'amount': _amount,
            'feature': _feature,
            'expire': _expire,
            'delay': _delay,
            'condition': _condition,
            'price': _price,
            'status': 'pending',
            'current_timestamp': self.get_current_timestamp(),
            'send_timestamp': None,
            'receive_timestamp': None,
            'last_timestamp': None,
            'local_timestamp': None,
            'execute_price': None,
            'execute_amount': 0,
            'fee_rate': None,
            'msg': None
        }
        return order_id

    def _remove_order(self, _order_id):
        self.__orders.pop(_order_id, None)

    def _remove_sniffer(self, _order_id):
        self.__sniffers.pop(_order_id, None)

    def _update_order_info(
            self,
            _order_id,
            _status,
            _last_timestamp,
            _local_timestamp,
            _execute_price=None,
            _execute_amount=None,
            _fee_rate=None,
            _msg=None
    ):
        order_info = self.__orders.get(_order_id)
        if order_info is None:
            return

        if self.ORDER_STATUS[_status] < self.ORDER_STATUS[order_info['status']]:
            return

        elif self.ORDER_STATUS[_status] == self.ORDER_STATUS[order_info['status']]:
            if _execute_amount <= (order_info['execute_amount'] or 0):
                return

        order_info['status'] = _status
        order_info['last_timestamp'] = float(_last_timestamp)
        order_info['local_timestamp'] = float(_local_timestamp)

        if _execute_price is not None:
            order_info['execute_price'] = float(_execute_price)

        if _execute_amount is not None:
            order_info['execute_amount'] = float(_execute_amount)

        if _fee_rate is not None:
            order_info['fee_rate'] = _fee_rate

        if _msg is not None:
            order_info['msg'] = _msg

        if self.ORDER_STATUS[_status] >= 5:
            self.__orders.pop(_order_id)

        return order_info

    def _update_sniffer_info(
            self,
            _order_id,
            _status,
            _last_timestamp,
            _local_timestamp,
    ):
        sniffer_info = self.__sniffers.get(_order_id)
        if sniffer_info is None:
            return

        if self.ORDER_STATUS[_status] < self.ORDER_STATUS[sniffer_info['status']]:
            return

        sniffer_info['status'] = _status
        sniffer_info['last_timestamp'] = float(_last_timestamp)
        sniffer_info['local_timestamp'] = float(_local_timestamp)

        if self.SNIFFER_STATUS[_status] >= 2:
            self.__sniffers.pop(_order_id)

        return sniffer_info


class StrategyBase:
    def __init__(self):
        self.order_mgr: Optional[OrderBase] = None
        self.logger: Optional[OnlineLogger] = None

    def init_mgr(self, _order_mgr, _logger_type):
        self.order_mgr = _order_mgr
        self.logger = OnlineLogger(_logger_type)

    @abc.abstractmethod
    async def on_signal(self, _signal):
        pass

    @abc.abstractmethod
    async def on_order(self, _order_info):
        pass

    @abc.abstractmethod
    async def on_sniffer(self, _sniffer_info):
        pass

    @abc.abstractmethod
    async def on_timer(self, _event):
        pass

    @abc.abstractmethod
    async def on_start(self, _event):
        pass

    @abc.abstractmethod
    async def on_stop(self, _event):
        pass
