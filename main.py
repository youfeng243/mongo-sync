#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: main.py
@time: 2017/8/15 17:05
"""
import threading

from common import log
from execute_task import RunTask
from generator_task import GenTask


def gen_task():
    log.info("任务生成线程开始运行...")
    GenTask(log)()


def run_task():
    log.info("任务执行线程开始运行...")
    RunTask(log)()


def main():
    log.info("启动mongo同步程序...")

    # 启动任务分割线程
    gen_thread = threading.Thread(target=gen_task)
    gen_thread.start()

    log.info("启动任务生成线程...")

    # 启动任务执行线程
    run_thread = threading.Thread(target=run_task)
    run_thread.start()

    log.info("启动任务执行线程...")

    run_thread.join()


if __name__ == '__main__':
    main()
