# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 16:58
@File: DefUtil.py
@Author: Jingyuan
"""
import abc
import uuid
from typing import Optional, List, Dict
from queue import PriorityQueue


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


class OrderBase:
    STATUS_DICT = {
        'pending': 0,
        'waiting': 1,
        'triggered': 2,
        'partial_filled': 3,
        'canceling': 4,
        'canceled': 5,
        'error': 5,
        'filled': 5
    }

    def __init__(self, _exec_dict):
        self.exec_dict = _exec_dict
        self.current_timestamp = None
        self._orders = {}
        self._sniffers = {}
        self._status_info = {}

    def update_current_timestamp(self, _timestamp):
        self.current_timestamp = _timestamp

    def get_order_info(self, _order_id):
        return self._orders.get(_order_id)

    def get_sniffer_info(self, _order_id):
        return self._sniffers.get(_order_id)

    def _update_send_timestamp(self, _order_id, _send_timestamp=None, _receive_timestamp=None):
        if _send_timestamp is not None:
            self._orders[_order_id]['send_timestamp'] = _send_timestamp

        if _receive_timestamp is not None:
            self._orders[_order_id]['receive_timestamp'] = _receive_timestamp

    def _generate_sniffer(
            self,
            _bind_id,
            _symbol,
            _exchange,
            _target_price,
            _operator,
            _order_type,
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
            'order_type': _order_type,
            'expire': _expire,
            'delay': _delay,
        }

        return order_id

    @abc.abstractmethod
    def update_events(self, _event_q: PriorityQueue):
        pass

    def _generate_order(
            self,
            _bind_id,
            _symbol,
            _exchange,
            _order_type,
            _action,
            _position,
            _feature=None,
            _expire=None,
            _delay=None,
            _condition=None,
            _price=None,
            _amount=None,
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
            'feature': _feature,
            'expire': _expire,
            'delay': _delay,
            'condition': _condition,
            'price': _price,
            'amount': _amount,
            'current_timestamp': self.current_timestamp
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
    def place_order(
            self,
            _bind_id,
            _symbol,
            _order_type,
            _action,
            _position,
            _feature=None,
            _expire=None,
            _delay=None,
            _condition=None,
            _price=None,
            _amount=None,
    ) -> str:
        pass

    @abc.abstractmethod
    def place_sniffer(
            self,
            _bind_id,
            _symbol,
            _operator,
            _target_price,
            _order_type,
            _expire=None,
            _delay=None
    ) -> str:
        pass

    @abc.abstractmethod
    def edit_order(self, _order_id, **kwargs):
        pass

    @abc.abstractmethod
    def edit_sniffer(self, _sniffer_id, **kwargs):
        pass

    @abc.abstractmethod
    def cancel_order(self, _order_id, _delay=None):
        pass

    @abc.abstractmethod
    def cancel_sniffer(self, _order_id, _delay=None):
        pass


class StrategyBase:
    def __init__(self):
        self.order_mgr: Optional[OrderBase] = None

    def update_order_mgr(self, _order_mgr):
        self.order_mgr = _order_mgr

    @abc.abstractmethod
    def on_signal(self, _signal):
        pass

    @abc.abstractmethod
    def on_order(self, _order_id):
        pass

    @abc.abstractmethod
    def on_sniffer(self, _sniffer_id):
        pass

    @abc.abstractmethod
    def on_timer(self, _event):
        pass

    @abc.abstractmethod
    def on_start(self, _event):
        pass

    @abc.abstractmethod
    def on_stop(self, _event):
        pass
