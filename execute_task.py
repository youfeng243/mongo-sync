#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: execute_task.py
@time: 2017/8/20 09:33
"""

# 任务执行类
from common import data_sync_db, app_data_db


class RunTask(object):
    def __init__(self, log):
        self.log = log
        self.data_sync_db = data_sync_db
        self.app_data_db = app_data_db

    def __call__(self, *args, **kwargs):
        self.start(*args, **kwargs)

    def start(self, *args, **kwargs):
        self.log.info("start 任务执行流程...")
