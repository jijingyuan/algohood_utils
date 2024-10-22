# -*- coding: utf-8 -*-
"""
@Create on  2024/10/21 8:10
@file: onlineLoggerUtil.py
@author: Jerry
"""
from collections import deque
from .loggerUtil import generate_logger


class OnlineLogger:

    def __init__(self, _type):
        self.type = _type
        self.logger = generate_logger()
        self.msg_q = deque()

    def debug(self, _msg):
        if self.type == 'local':
            self.logger.debug(_msg)
        elif self.type == 'cluster':
            pass
        elif self.type == 'online':
            pass
        elif self.type == 'sim':
            pass
        elif self.type == 'debug':
            self.logger.debug(_msg)

    def info(self, _msg):
        if self.type == 'local':
            self.logger.info(_msg)
        elif self.type == 'cluster':
            pass
        elif self.type == 'online':
            self.msg_q.append({'type': 'info', 'msg': _msg})
        elif self.type == 'sim':
            pass
        elif self.type == 'debug':
            self.logger.info(_msg)

    def error(self, _msg):
        if self.type == 'local':
            self.logger.error(_msg)
        elif self.type == 'cluster':
            self.logger.error(_msg)
        elif self.type == 'online':
            self.msg_q.append({'type': 'error', 'msg': _msg})
        elif self.type == 'sim':
            self.msg_q.append({'type': 'error', 'msg': _msg})
        elif self.type == 'debug':
            self.logger.error(_msg)
