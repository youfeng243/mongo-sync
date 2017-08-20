#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: common.py
@time: 2017/8/20 09:35
"""
import datetime
import time

import config
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


# 获得当前时间
def get_now_time():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# 获得时间戳
def get_time_stamp(time_str):
    return int(time.mktime(time.strptime(time_str, '%Y-%m-%d %H:%M:%S')))


# 从时间戳转换为 格式化时间
def get_format_time(time_stamp):
    time_local = time.localtime(time_stamp)
    return time.strftime("%Y-%m-%d %H:%M:%S", time_local)


# 获得之前的日期
def get_before_day(days):
    today = datetime.date.today()
    one_day = datetime.timedelta(days=days)
    yesterday = today - one_day
    return yesterday.strftime("%Y-%m-%d")


# 获得待同步表信息
def get_table_list():
    table_list = list()
    try:
        with open(config.TABLE_CONFIG) as p_file:
            for line in p_file:
                table_name = line.strip("\n").strip("\r").strip()
                table_list.append(table_name)
    except Exception as e:
        log.error("读取表配置信息错误...")
        log.exception(e)

    return table_list
