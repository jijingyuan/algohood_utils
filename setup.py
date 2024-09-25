# -*- coding: utf-8 -*-
"""
@Create: 2024/9/10 10:46
@File: setup.py
@Author: Jingyuan
"""

from setuptools import setup, find_packages

setup(
    name="algoUtils",  # 包的名字
    version="0.1",  # 版本号
    author="jingyuan",  # 作者名字
    author_email="jijingyuan@rookiequant.com",  # 作者邮箱
    description="utils for algo modules",  # 包的简短描述
    packages=find_packages(),  # 自动发现包目录
    install_requires=[
        "influxdb-client==1.46.0",
        'aiocsv',
        'aiohttp'
    ],
)
