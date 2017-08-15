#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: main.py
@time: 2017/8/15 17:05
"""
from logger import Logger

log = Logger("mongo-sync.log").get_logger()


def main():
    log.info("启动mongo同步程序...")


if __name__ == '__main__':
    main()
