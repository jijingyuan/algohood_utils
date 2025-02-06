# AlgoHood Utils

A comprehensive utility package providing essential tools and functionalities for the AlgoHood quantitative trading system.

## Overview

AlgoHood Utils is a collection of utility modules designed to support various components of the AlgoHood trading system. It provides functionalities for asynchronous communication, data management, logging, and time-related operations.

## Features

- **Asynchronous Communication**
  - `asyncQuicUtil.py`: QUIC protocol implementation for high-performance networking
  - `asyncRedisUtil.py`: Asynchronous Redis operations
  - `asyncZmqUtil.py`: Asynchronous ZeroMQ messaging
  
- **Data Management**
  - `redisUtil.py`: Redis database operations
  - `InfluxDBUtil.py`: Time-series data management with InfluxDB
  
- **Logging and Monitoring**
  - `loggerUtil.py`: General logging functionality
  - `onlineLoggerUtil.py`: Real-time logging system
  
- **Time and Date Operations**
  - `dateUtil.py`: Date and time manipulation utilities
  
- **System Utilities**
  - `defUtil.py`: Common definitions and utility functions
  - `reloadUtil.py`: Module reloading functionality

## Installation

```bash
pip install -e .
```

## Dependencies

Major dependencies include:
- aioquic==1.2.0
- redis==5.0.8
- pandas==1.5.3
- numpy==1.24.2
- aiohttp==3.10.6
- pyzmq==26.2.0
- influxdb-client==1.46.0
- scikit-learn==1.6.1
- lightgbm==4.5.0

For a complete list of dependencies, please refer to `setup.py`.

## Usage

```python
# Example: Using Redis Utility
from algoUtils import asyncRedisUtil

# Example: Using Logger
from algoUtils import loggerUtil

# Example: Using Date Utility
from algoUtils import dateUtil
```

## Contributing

Please follow the project's coding standards and submit pull requests for any new features or bug fixes.

## License

Proprietary - All rights reserved

## Author

Jingyuan (jijingyuan@rookiequant.com)
