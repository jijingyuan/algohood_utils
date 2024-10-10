# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 16:58
@File: DefUtil.py
@Author: Jingyuan
"""
import abc
import uuid
from typing import Optional, List, Dict
import importlib


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
            'type': 'start' or 'end' or 'isolated' or 'consecutive',
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

    def __init__(self, _method_name, _method_param):
        self.exec_mgr = self.__get_exec_method(_method_name, _method_param)
        self.current_timestamp = None
        self.__orders = {}
        self.__sniffers = {}
        self.__status_info = {}

    @staticmethod
    def __get_exec_method(_method_name, _method_param):
        if _method_param is None:
            _method_param = {}
        module = importlib.import_module('algoExecution.algoExec.{}'.format(_method_name.lower()))
        cls_method = getattr(module, _method_name)
        if cls_method is None:
            raise Exception('Unknown Method: {}'.format(_method_name))
        instance = cls_method(**_method_param)
        return instance

    def update_current_timestamp(self, _timestamp):
        self.current_timestamp = _timestamp

    def __update_send_timestamp(self, _order_id, _send_timestamp=None, _receive_timestamp=None):
        if _send_timestamp is not None:
            self.__orders[_order_id]['send_timestamp'] = _send_timestamp

        if _receive_timestamp is not None:
            self.__orders[_order_id]['receive_timestamp'] = _receive_timestamp

    def __generate_sniffer(
            self,
            _bind_id,
            _symbol,
            _target_price,
            _operator,
            _order_type,
            _expire=None,
            _delay=None
    ):

        order_id = str(uuid.uuid4())
        self.__sniffers[order_id] = {
            'bind_id': _bind_id,
            'order_id': order_id,
            'symbol': _symbol,
            'target_price': _target_price,
            'operator': _operator,
            'order_type': _order_type,
            'expire': _expire,
            'delay': _delay,
        }

        return order_id

    def __generate_order(
            self,
            _bind_id,
            _symbol,
            _order_type,
            _action,
            _position,
            _feature=None,
            _expire=None,
            _delay=None,
            _target_price=None,
            _operator=None,
            _condition_expire=None,
            _price=None,
            _amount=None,
            _client_id=None,
    ):
        order_id = str(uuid.uuid4())
        self.__orders[order_id] = {
            'bind_id': _bind_id,
            'order_id': order_id,
            'client_id': _client_id,
            'symbol': _symbol,
            'order_type': _order_type,
            'action': _action,
            'position': _position,
            'feature': _feature,
            'expire': _expire,
            'delay': _delay,
            'target_price': _target_price,
            'operator': _operator,
            'condition_expire': _condition_expire,
            'price': _price,
            'amount': _amount,
            'current_timestamp': self.current_timestamp
        }
        return order_id

    @abc.abstractmethod
    def __update_status_info(
            self,
            _order_id,
            _status,
            _last_timestamp,
            _local_timestamp,
            _execute_price=None,
            _execute_amount=None
    ):
        pass

    @abc.abstractmethod
    def create_order(
            self,
            _bind_id,
            _symbol,
            _order_type,
            _action,
            _position,
            _feature=None,
            _expire=None,
            _delay=None,
            _target_price=None,
            _operator=None,
            _condition_expire=None,
            _price=None,
            _amount=None,
    ) -> str:
        pass

    @abc.abstractmethod
    def create_sniffer(
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
    def cancel_sniffer(self, _sniffer_id, _delay=None):
        pass


class StrategyBase:
    def __init__(self):
        self.order_mgr: Optional[OrderBase] = None

    def update_on_event(self, _order_mgr):
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


class Order:
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

    def __init__(
            self,
            _order_id,
            _bind_id,
            _symbol,
            _order_type,
            _action,
            _position,
            _feature=None,
            _expire=None,
            _delay=None,
            _target_price=None,
            _operator=None,
            _condition_expire=None,
            _price=None,
            _amount=None
    ):
        self.order_id = _order_id
        self.__bind_id = _bind_id
        self.__symbol = _symbol
        self.__order_type = _order_type
        self.__action = _action
        self.__position = _position
        self.__feature = _feature
        self.__strategy_id = None
        self.__account_name = None
        self.__expire = _expire
        self.__delay = _delay
        self.__target_price = _target_price
        self.__operator = _operator
        self.__condition_expire = _condition_expire
        self.__price = _price
        self.__amount = _amount
        self.__execute_amount = None
        self.__send_timestamp = None
        self.__receive_timestamp = None
        self.__status = 'pending'
        self.__execute_price = None
        self.__last_timestamp = None
        self.__local_timestamp = None
        self.__close_timestamp = None

    def update_timestamp(self, _send_timestamp=None, _receive_timestamp=None):
        if _send_timestamp:
            self.__send_timestamp = _send_timestamp
        if _receive_timestamp:
            self.__receive_timestamp = _receive_timestamp

    def update_account_name(self, _account_name):
        self.__account_name = _account_name

    def update_strategy_id(self, _strategy_id):
        self.__strategy_id = _strategy_id

    def update_status(self, _status, _last_timestamp, _local_timestamp, _execute_price, _execute_amount):
        if self.STATUS_DICT[_status] < self.STATUS_DICT[self.__status]:
            return

        self.__status = _status
        self.__last_timestamp = _last_timestamp
        self.__local_timestamp = _local_timestamp
        self.__execute_price = _execute_price
        self.__execute_amount = _execute_amount
        if self.STATUS_DICT[_status] >= 5:
            self.__close_timestamp = _last_timestamp

    @property
    def order_info(self):
        return {k.replace('__', ''): v for k, v in self.__dict__.items() if v is not None}


class Sniffer:
    def __init__(
            self, _sniffer_id, _bind_id, _symbol, _operator, _target_price, _expire=None, _delay=None
    ):
        self.__sniffer_id = _sniffer_id
        self.__bind_id = _bind_id
        self.__symbol = _symbol
        self.__operator = _operator
        self.__target_price = _target_price
        self.__expire = None
        self.__delay = None
        self.__status = 'pending'
        self.__last_timestamp = None

    def update_sniffer_feature(self, _expire=None, _delay=None):
        self.__expire = _expire
        self.__delay = _delay

    def update_status(self, _status, _last_timestamp):
        self.__status = _status
        self.__last_timestamp = _last_timestamp

    @property
    def sniffer_info(self):
        return {k.replace('__', ''): v for k, v in self.__dict__.items() if v is not None}
