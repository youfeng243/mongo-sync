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
import time

import common
from common import data_sync_db, app_data_db


class RunTask(object):
    SLEEP_PERIOD_TIME = 3 * 60

    def __init__(self, log):
        self.log = log
        self.data_sync_db = data_sync_db
        self.app_data_db = app_data_db
        self.app_data_table_list = common.get_table_list()
        self.data_sync_task_table_list = common.get_task_table_list(self.app_data_table_list)
        self.data_sync_del_table_list = common.get_del_table_list(self.app_data_table_list)

    def __call__(self, *args, **kwargs):
        self.start(*args, **kwargs)

    # 开始同步数据
    def sync_data(self):
        self.log.info('开始同步数据...')

        # 按表遍历同步数据
        pass

    def start(self, *args, **kwargs):
        self.log.info("start 任务执行流程...")

        while True:
            try:
                self.sync_data()

                # 休眠
                self.log.info("任务执行周期完成, 开始休眠...zzz")
                time.sleep(self.SLEEP_PERIOD_TIME)
            except Exception as e:
                self.log.error("数据同步任务执行周期异常: ")
                self.log.exception(e)
                time.sleep(3600)
