#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: delete_all_task_table.py
@time: 2017/8/20 15:22
"""
import common
from common import data_sync_db


def main():
    for table_name in common.get_task_table_list(common.get_table_list()):
        common.log.info("当前删除的表: {}".format(table_name))
        data_sync_db.drop(table_name)


if __name__ == '__main__':
    main()
