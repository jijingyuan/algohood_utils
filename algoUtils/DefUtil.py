# -*- coding: utf-8 -*-
"""
@Create: 2024/9/30 16:58
@File: DefUtil.py
@Author: Jingyuan
"""


class Order:

    def __init__(
            self,
            _bind_id,
            _symbol,
            _order_type,
            _action,
            _position,
            _feature,
    ):
        self.__bind_id = _bind_id
        self.__symbol = _symbol
        self.__order_type = _order_type
        self.__action = _action
        self.__position = _position
        self.__feature = _feature
        self.__strategy_id = None
        self.__account_name = None
        self.__expire = None
        self.__delay = None
        self.__target_price = None
        self.__operator = None
        self.__condition_expire = None
        self.__status = None
        self.__price = None
        self.__execute_price = None

    def set_order_feature(
            self,
            _expire=None,
            _delay=None,
            _target_price=None,
            _operator=None,
            _condition_expire=None,
            _price=None
    ):
        self.__expire = _expire
        self.__delay = _delay
        self.__target_price = _target_price
        self.__operator = _operator
        self.__condition_expire = _condition_expire
        self.__price = _price

    def set_account_name(self, _account_name):
        self.__account_name = _account_name

    def set_strategy_id(self, _strategy_id):
        self.__strategy_id = _strategy_id

    @property
    def order_info(self):
        return {k.replace('__', ''): v for k, v in self.__dict__.items() if v is not None}


class Sniffer:
    def __init__(self):
        pass

    @property
    def sniffer_info(self):
        return {k.replace('__', ''): v for k, v in self.__dict__.items() if v is not None}


class Timer:
    def __init__(self):
        pass

    @property
    def timer_info(self):
        return
