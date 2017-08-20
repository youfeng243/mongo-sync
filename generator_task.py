#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: generator_task.py
@time: 2017/8/20 09:31
"""

# 任务生成类
import time

import common
import config
from common import data_sync_db
from config import CHECK_PERIOD


class GenTask(object):
    PERIOD_TIME = 4 * 3600

    def __init__(self, log):
        self.log = log
        self.data_sync_db = data_sync_db
        # 任务表
        self.task_table_list = [config.SYNC_TABLE_FLAG + x for x in common.get_table_list()]
        self.log.info("当前需要同步的任务表:")
        self.log.info(self.task_table_list)

    def __call__(self, *args, **kwargs):
        self.start(*args, **kwargs)

    # 生成任务信息
    def gen_task_item(self, _id):

        task = {
            '_id': _id,
            'finish': False,
            '_in_time': common.get_now_time(),
            '_utime': common.get_now_time(),
            'task': list(),
        }

        segment = 5 * 60
        start_time = _id + " 00:00:00"
        end_time = _id + " 23:59:59"
        start_time = common.get_time_stamp(start_time)
        end_time = common.get_time_stamp(end_time)
        self.log.info('当前计算时间段: {} - {}'.format(start_time, end_time))
        while start_time <= end_time:
            temp_time = start_time + segment - 1

            start_time_str = common.get_format_time(start_time)
            end_time_str = common.get_format_time(temp_time)

            task_item = {
                'start_time': start_time_str,
                'end_time': end_time_str,
                'finish': False,
                'error_times': 0,
                '_utime': common.get_now_time(),
            }
            task['task'].append(task_item)
            self.log.info('当前任务时间段: {} - {}'.format(start_time_str, end_time_str))

            start_time += segment
        return task

    # 生成当日任务
    def gen_daily_task(self):
        self.log.info("开始生成每日任务...")

        # 遍历所有的任务表
        for table_name in self.task_table_list:
            for days in xrange(CHECK_PERIOD, -1, -1):
                _id = common.get_before_day(days)
                self.log.info("当前检测日期: {} {} {}".format(table_name, days, _id))
                task_item = self.data_sync_db.find_one(table_name, {'_id': _id})
                if task_item is not None:
                    continue
                task_item = self.gen_task_item(_id)

                # 存储任务信息
                self.data_sync_db.insert(table_name, task_item)

    def del_daily_task(self):
        self.log.info("开始删除周期外已完成的任务...")

        # 获得最远的日期信息
        last_pass_day = common.get_before_day(CHECK_PERIOD)
        self.log.info("当前需要删除小于日期: {} 的任务信息".format(last_pass_day))

        for table_name in self.task_table_list:
            for item in self.data_sync_db.traverse(table_name, {"_id": {"$lt": last_pass_day}}):
                try:
                    _id = item['_id']
                    finish = item.get('finish')
                    if finish is None or finish == True:
                        self.log.info("当前需要删除已完成周期外任务信息: {} {}".format(table_name, item['_id']))
                        self.data_sync_db.delete(table_name, {'_id': _id})
                except Exception as e:
                    self.log.error('删除任务信息失败:')
                    self.log.exception(e)

        self.log.info("周期外已完成的任务删除完成...")

    def start(self, *args, **kwargs):
        self.log.info("start 任务生成流程...")

        while True:
            try:
                # 生成任务
                self.gen_daily_task()

                # 删除周期外已经完成的任务
                self.del_daily_task()

                # 休眠
                self.log.info("任务生成与检测已完成周期执行, 开始休眠...zzz")
                time.sleep(self.PERIOD_TIME)
            except Exception as e:
                self.log.error('生成任务线程异常: ')
                self.log.exception(e)
                time.sleep(3600)
