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
from common import data_sync_db, app_data_db, m2_db
from mongo import MongDb


class RunTask(object):
    SLEEP_PERIOD_TIME = 3 * 60

    # 最大错误次数
    MAX_ERROR_TIMES = 3

    # 批处理数目
    MAX_BATCH_SIZE = 500

    def __init__(self, log):
        self.log = log
        self.data_sync_db = data_sync_db
        self.app_data_db = app_data_db
        self.m2_db = m2_db
        self.app_data_table_list = common.get_table_list()
        self.data_sync_task_table_list = common.get_task_table_list(self.app_data_table_list)
        self.data_sync_del_table_list = common.get_del_table_list(self.app_data_table_list)

        # 创建索引
        self.create_index()

    def __call__(self, *args, **kwargs):
        self.start(*args, **kwargs)

    # 创建索引
    def create_index(self):
        self.log.info("开始给删除记录表创建索引...")
        for table_name in self.data_sync_del_table_list:
            self.data_sync_db.create_index(table_name, [('_utime', MongDb.ASCENDING)])

        self.log.info("删除记录表索引创建完成...")

    # 同步修改记录
    def sync_update_data(self, table_name, update_item, start_time, end_time):
        result_list = list()

        # 如果已经完成了修改操作同步 也不在进行同步
        finish = update_item.get('finish')
        if finish:
            return True

        error_times = update_item.get('error_times', 0)
        if error_times >= self.MAX_ERROR_TIMES:
            self.log.error("同步超过最大错误次数，不再进行同步: {} {} - {}".format(
                table_name, start_time, end_time))
            return False

        try:
            for item in self.app_data_db.traverse_batch(table_name, {'_utime':
                                                                         {"$gte": start_time,
                                                                          "$lte": end_time}
                                                                     }):
                item['logic_delete'] = 0
                result_list.append(item)
                if len(result_list) >= self.MAX_BATCH_SIZE:
                    self.m2_db.insert_batch_data(table_name, result_list)
                    del result_list[:]

            self.m2_db.insert_batch_data(table_name, result_list)
            finish = True
        except Exception as e:
            self.log.error('修改数据同步出错: {} {} - {}'.format(table_name, start_time, end_time))
            self.log.exception(e)
            finish = False
            update_item['error_times'] = error_times + 1

        # 更新时间
        update_item['_utime'] = common.get_now_time()
        update_item['finish'] = finish

        return finish

    # 同步删除记录
    def sync_delete_data(self, table_name, delete_item, start_time, end_time):

        # 获得删除表表名
        delete_table_name = common.get_del_table_name(table_name)

        # 如果已经完成了删除操作同步 也不在进行同步
        finish = delete_item.get('finish')
        if finish:
            return True

        error_times = delete_item.get('error_times', 0)
        if error_times >= self.MAX_ERROR_TIMES:
            self.log.error("同步超过最大错误次数，不再进行同步: {} {} - {}".format(
                table_name, start_time, end_time))
            return False

        result_list = list()
        try:
            # 这里查找删除记录表
            for item in self.data_sync_db.traverse_batch(delete_table_name, {'_utime':
                                                                                 {"$gte": start_time,
                                                                                  "$lte": end_time}}):
                _record_id = item.get('_record_id')
                if _record_id is None:
                    self.log.error('没有主键，无法进行逻辑删除: {}'.format(item))
                    continue

                # 根据主键查找对应的M1数据表
                data_item = self.app_data_db.find_one(table_name, {'_record_id': _record_id})
                if data_item is None:
                    continue

                # 设置逻辑删除字段 为 1 已经删除该字段
                data_item['logic_delete'] = 1

                result_list.append(data_item)
                if len(result_list) >= self.MAX_BATCH_SIZE:
                    self.m2_db.insert_batch_data(table_name, result_list)
                    del result_list[:]

            self.m2_db.insert_batch_data(table_name, result_list)
            finish = True
        except Exception as e:
            self.log.error('删除数据同步出错: {} {} - {}'.format(table_name, start_time, end_time))
            self.log.exception(e)
            finish = False
            delete_item['error_times'] = error_times + 1

        # 更新时间
        delete_item['_utime'] = common.get_now_time()
        delete_item['finish'] = finish

        return finish

    # 同步单个表信息
    def sync_table_task(self, table_name, task, sync_time):

        # 获得任务表名称
        task_table_name = common.get_task_table_name(table_name)

        task_list = task.get('task_list')
        # 遍历所有的时间段
        for task_item in task_list:
            # 如果是已经完成的时间段
            if task_item.get('finish'):
                continue

            start_time = task_item.get('start_time')
            end_time = task_item.get('end_time')

            # 判断当前时间段是否适合同步
            if end_time > sync_time:
                continue

            # 有操作则先更新时间
            task_item['_utime'] = common.get_now_time()

            # 获得更新数据的任务信息
            update_item = task_item.get('update')

            # 如果同步修改出错 则中断同步, 需要查出问题 再进行同步，否则会导致数据不一致
            if not self.sync_update_data(table_name, update_item, start_time, end_time):
                self.data_sync_db.save(task_table_name, task)
                return

            # 获得删除数据的任务信息
            delete_item = task_item.get('delete')
            if not self.sync_delete_data(table_name, delete_item, start_time, end_time):
                self.data_sync_db.save(task_table_name, task)
                return

            # 流程能跑到这里，必然确定 修改的数据与删除的数据都已经同步到M2
            task_item['finish'] = True

            # 存储任务执行记录
            self.data_sync_db.save(task_table_name, task)

        # 这里需要判断是否所有的任务都已经完成了
        all_finish = True
        for task_item in task_list:
            if not task_item.get('finish'):
                all_finish = False
                break

        # 如果流程执行到最下面，则证明所有的同步都成功了
        task['finish'] = all_finish
        task['_utime'] = common.get_now_time()
        self.data_sync_db.save(task_table_name, task)

    # 开始同步数据
    def sync_data(self):
        self.log.info('开始同步数据...')

        # 先获得当前时间点
        current_time = common.get_format_time(common.get_time_stamp(common.get_now_time()) - self.SLEEP_PERIOD_TIME)
        self.log.info("当前需要同步的时间切面为: {}".format(current_time))

        # 遍历任务表
        for table_name in self.app_data_table_list:

            task_table_name = common.get_task_table_name(table_name)
            self.log.info('当前同步的表名称: {}'.format(table_name))
            self.log.info('当前同步的任务表名称: {}'.format(task_table_name))

            # 按表遍历同步数据, 从时间最久远开始遍历
            for task in self.data_sync_db.traverse_sort(task_table_name,
                                                        {'finish': {'$ne': True}},
                                                        [("_id", MongDb.ASCENDING)]):
                self.sync_table_task(table_name, task, current_time)

        self.log.info('当前时间截面数据同步完成...')

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
