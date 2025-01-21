# -*- coding: utf-8 -*-
"""
@Create: 2024/11/29 12:28
@File: reloadUtil.py
@Author: Jingyuan
"""
import types
from importlib import reload


def walk_module(_module, _map):
    if _module not in _map:
        _map[_module] = None
        reload(_module)


def reload_all(*modules):  # 需要重载的模块
    visited_map = {}
    for module in modules:
        if isinstance(module, types.ModuleType):  # 如果是一个模块对象的话
            walk_module(module, visited_map)
