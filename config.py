#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: config.py
@time: 2017/7/1 14:17
"""
# 业务数据库
APP_DATA_CONFIG = {
    "host": "172.16.215.16",
    "port": 40042,
    "db": "app_data",
    "username": "read",
    "password": "read",
}

TARGET_CONFIG = {
    "host": "172.16.215.2",
    "port": 40042,
    "db": "test",
    "username": "test",
    "password": "test",
}

# 数据同步记录
DATA_SYNC_CONFIG = {
    "host": "172.16.215.2",
    "port": 40042,
    "db": "data_sync",
    "username": "work",
    "password": "haizhi",
}

# 校验周期
CHECK_PERIOD = 3

# 删除记录表前置标识
DELETE_TABLE_FLAG = "delete_"

# 同步记录表
SYNC_TABLE_FLAG = "sync_"

# 数据表配置信息
TABLE_CONFIG = "table.config"
