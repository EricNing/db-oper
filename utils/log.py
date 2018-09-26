# -*- coding:utf-8 -*-
import logging
import logging.handlers

def get_logger(logfile):
    # 创建一个日志器logger并设置其日志级别为INFO
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.INFO)

    # 创建一个handler(日志文件按大小切割),并设置其日志级别为INFO
    rf_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=10 * 1024 * 1024, backupCount=5)
    # rf_handler.setLevel(logging.INFO)

    # 创建一个格式器formatter并将其添加到处理器handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    rf_handler.setFormatter(formatter)

    # 创建一个handler(日志文件按时间切割),并设置其日志级别为INFO
    # rf_handler = logging.handlers.TimedRotatingFileHandler(logfile, when='midnight', interval=1, backupCount=7, atTime=datetime.time(0, 0, 0, 0))
    # rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    #
    # f_handler = logging.FileHandler('error.log')
    # f_handler.setLevel(logging.ERROR)
    # f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))

    logger.addHandler(rf_handler)
    return logger