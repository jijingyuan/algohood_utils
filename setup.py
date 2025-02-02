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
        'aioquic==1.2.0',
        'psutil==5.9.4',
        'requests==2.32.0',
        'ujson==5.7.0',
        'numpy==1.24.2',
        'pandas==1.5.3',
        'websockets==13',
        'redis==5.0.8',
        'influxdb-client==1.46.0',
        'aiocsv==1.3.2',
        'aiohttp==3.10.6',
        'pyzmq==26.2.0',
        'tornado==6.4.1',
        'aioredis==1.3.1',
        'aiofiles==24.1.0',
        'msgspec==0.18.6',
        'sklearn==1.3.1',
        'lightgbm==4.1.1'
    ],
)
