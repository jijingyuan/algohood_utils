# AlgoHood Utils

量化交易系统的核心工具包

## 概述

AlgoHood Utils 是一个综合性工具包，为 AlgoHood 量化交易系统的各个组件提供基础支持。它提供了异步通信、数据管理、日志记录和时间操作等核心功能。

## 功能特性

### 🔄 异步通信
- **异步 QUIC 工具 (`asyncQuicUtil.py`)**
  - 高性能网络通信协议实现
  - 低延迟数据传输
  - 可靠的连接管理

- **异步 Redis 工具 (`asyncRedisUtil.py`)**
  - 异步 Redis 操作接口
  - 高效的缓存管理
  - 分布式数据同步

- **异步 ZMQ 工具 (`asyncZmqUtil.py`)**
  - 消息队列管理
  - 发布订阅模式支持
  - 高吞吐量通信

### 📊 数据管理
- **Redis 工具 (`redisUtil.py`)**
  - Redis 数据库操作
  - 缓存策略管理
  - 数据持久化

- **InfluxDB 工具 (`InfluxDBUtil.py`)**
  - 时序数据管理
  - 性能指标存储
  - 数据查询优化

### 📝 日志与监控
- **日志工具 (`loggerUtil.py`)**
  - 统一日志记录
  - 错误追踪
  - 性能监控

- **在线日志工具 (`onlineLoggerUtil.py`)**
  - 实时日志系统
  - 状态监控
  - 警报管理

### ⏰ 时间工具
- **日期工具 (`dateUtil.py`)**
  - 时间格式转换
  - 交易日历管理
  - 时区处理

### 🛠 系统工具
- **通用工具 (`defUtil.py`)**
  - 常用函数和定义
  - 工具类封装
  - 通用接口

- **重载工具 (`reloadUtil.py`)**
  - 模块热重载
  - 配置更新
  - 动态加载

## 安装

```bash
pip install -e .
```

## 依赖项

主要依赖包括：
- aioquic==1.2.0
- redis==5.0.8
- pandas==1.5.3
- numpy==1.24.2
- aiohttp==3.10.6
- pyzmq==26.2.0
- influxdb-client==1.46.0
- scikit-learn==1.6.1
- lightgbm==4.5.0

完整依赖列表请参考 `setup.py`。

## 使用示例

```python
# 使用 Redis 工具
from algoUtils import asyncRedisUtil

# 使用日志工具
from algoUtils import loggerUtil

# 使用日期工具
from algoUtils import dateUtil
```

## 工具说明

### 异步通信工具
- 提供高性能的网络通信支持
- 支持多种协议和通信模式
- 优化的异步操作接口

### 数据管理工具
- 统一的数据访问接口
- 高效的数据存储和检索
- 完善的缓存机制

### 日志监控工具
- 全面的日志记录功能
- 实时的系统监控
- 灵活的配置选项

### 时间处理工具
- 精确的时间管理
- 交易日历支持
- 多时区处理

## 配置说明

各工具模块都提供了灵活的配置选项，可以通过参数调整来满足不同的使用需求。

## 参与贡献

请遵循项目的编码标准，并为任何新功能或错误修复提交拉取请求。

## 许可证

专有软件 - 保留所有权利

## 作者

Jingyuan (jijingyuan@rookiequant.com) 