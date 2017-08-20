#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: common.py
@time: 2017/8/20 09:35
"""
from config import APP_DATA_CONFIG, DATA_SYNC_CONFIG
from logger import Logger
from mongo import MongDb

log = Logger("mongo-sync.log").get_logger()

# 待同步数据db
app_data_db = MongDb(APP_DATA_CONFIG['host'], APP_DATA_CONFIG['port'], APP_DATA_CONFIG['db'],
                     APP_DATA_CONFIG['username'],
                     APP_DATA_CONFIG['password'], log=log)

# 同步信息记录db
data_sync_db = MongDb(DATA_SYNC_CONFIG['host'], DATA_SYNC_CONFIG['port'], DATA_SYNC_CONFIG['db'],
                      DATA_SYNC_CONFIG['username'],
                      DATA_SYNC_CONFIG['password'], log=log)
