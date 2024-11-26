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
            'bind_id': str(uuid),
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


class OptimizeBase:

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

    def __init__(self, _exec_dict):
        self.exec_dict = _exec_dict
        self.precision_dict = {}
        self.current_timestamp = None
        self._orders = {}
        self._sniffers = {}
        self._status_info = {}
        self._price = {}

    def update_current_timestamp(self, _timestamp):
        self.current_timestamp = _timestamp

    def update_symbol_price(self, _symbol, _price):
        self._price[_symbol] = _price

    def get_order_info(self, _order_id):
        return self._orders.get(_order_id)

    def get_sniffer_info(self, _order_id):
        return self._sniffers.get(_order_id)

    async def update_precision_dict(self, _timestamp, _symbol):
        if self.precision_dict.get(_symbol) is not None:
            return

        pair, exchange = _symbol.split('|')
        exec_mgr = self.exec_dict[exchange]
        data = await exec_mgr.data_mgr.get_data_given_start_amount('trade', _symbol, _timestamp, 10)

        close_list = []
        amount_list = []
        for x in data.values():
            for y in x:
                for k, v in y.items():
                    if 'amount' in k:
                        amount_list.extend([amount[1] for amount in v[1]])
                    else:
                        close_list.extend([close[1] for close in v[1]])

        self.precision_dict[_symbol] = {
            'price': self.max_decimal_places(close_list),
            'amount': self.max_decimal_places(amount_list)
        }

    @staticmethod
    def max_decimal_places(numbers) -> int:
        decimals = [Decimal(str(num)) for num in numbers]
        res = max(-d.as_tuple().exponent if d.as_tuple().exponent < 0 else 0 for d in decimals)
        return res

    def format_amount(self, _symbol, _amount, _upper=True):
        amount_p = self.precision_dict[_symbol]['amount']
        factor = 10 ** amount_p
        int_amount = int(_amount * factor)
        bias = 1 if _upper else 0
        return round((int_amount + bias) / factor, amount_p)

    def format_price(self, _symbol, _price, _upper=True):
        amount_p = self.precision_dict[_symbol]['price']
        factor = 10 ** amount_p
        int_amount = int(_price * factor)
        bias = 1 if _upper else 0
        return round((int_amount + bias) / factor, amount_p)

    def _update_send_timestamp(self, _order_id, _send_timestamp=None, _receive_timestamp=None):
        if _send_timestamp is not None:
            self._orders[_order_id]['send_timestamp'] = _send_timestamp

        if _receive_timestamp is not None:
            self._orders[_order_id]['receive_timestamp'] = _receive_timestamp

    def _generate_target_sniffer(
            self,
            _bind_id,
            _symbol,
            _exchange,
            _target_price,
            _operator,
            _expire=None,
            _delay=None
    ):

        order_id = str(uuid.uuid4())
        _, exchange = _symbol.split('|')
        self._sniffers[order_id] = {
            'bind_id': _bind_id,
            'order_id': order_id,
            'symbol': _symbol,
            'exchange': _exchange,
            'target_price': _target_price,
            'operator': _operator,
            'expire': _expire,
            'delay': _delay,
        }

        return order_id

    def _generate_trailing_sniffer(
            self,
            _bind_id,
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
        self._sniffers[order_id] = {
            'bind_id': _bind_id,
            'order_id': order_id,
            'symbol': _symbol,
            'exchange': _exchange,
            'operator': _operator,
            'back_pct': _back_pct,
            '_smooth': _smooth,
            'expire': _expire,
            'delay': _delay,
        }

        return order_id

    @abc.abstractmethod
    async def update_events(self, _event_q: PriorityQueue):
        pass

    def _generate_order(
            self,
            _bind_id,
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
            _client_id=None,
    ):
        order_id = str(uuid.uuid4())
        self._orders[order_id] = {
            'bind_id': _bind_id,
            'order_id': order_id,
            'client_id': _client_id,
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
            'current_timestamp': self.current_timestamp,
            'execute_price': None,
            'execute_amount': 0
        }
        return order_id

    @abc.abstractmethod
    def update_status_info(
            self,
            _order_id,
            _order_type,
            _status,
            _last_timestamp,
            _local_timestamp,
            _execute_price=None,
            _execute_amount=None
    ):
        pass

    @abc.abstractmethod
    async def place_order(
            self,
            _bind_id,
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
            _bind_id,
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
            _bind_id,
            _symbol,
            _operator,
            _back_pct,
            _smooth=None,
            _expire=None,
            _delay=None
    ) -> str:
        pass

    @abc.abstractmethod
    async def edit_order(self, _order_id, **kwargs):
        pass

    @abc.abstractmethod
    async def edit_sniffer(self, _sniffer_id, **kwargs):
        pass

    @abc.abstractmethod
    async def cancel_order(self, _order_id, _delay=None):
        pass

    @abc.abstractmethod
    async def cancel_sniffer(self, _order_id, _delay=None):
        pass

    @abc.abstractmethod
    def get_trading_amount(self, _symbol):
        pass


class StrategyBase:
    def __init__(self):
        self.order_mgr: Optional[OrderBase] = None
        self.logger: Optional[OnlineLogger] = None

    def update_order_mgr(self, _order_mgr):
        self.order_mgr = _order_mgr

    def update_logger(self, _type):
        self.logger = OnlineLogger(_type)

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
