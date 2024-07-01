"""
@File : __init__.py
@Date : 2024/6/27 上午11:27
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import logging

import datetime
import os

from config import CONFIG
from logging.handlers import RotatingFileHandler

"""这是简易的日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    # filename='log.txt',  注释后不会生成日志文件
    # filemode='a'  # w 表示覆盖，a 表示追加
)
logger = logging.getLogger(__name__)
"""

# 为避免其他次级目录的访问，需要设置模块所在的完整路径
current_dir = os.path.dirname(os.path.abspath(__file__))

log_file_path = os.path.join(current_dir, CONFIG["Log"]["FilePath"])


"""常规日志配置"""


# 创建一个 logger
def init_logger():
    # 创建一个 logger
    root_logger = logging.getLogger(__name__)
    # 设置日志级别
    root_logger.setLevel(CONFIG['Log']['Level'])

    # 根据日志输出类型创建不同的输出渠道
    if CONFIG['Log']['Output'] == 'console':
        log_handler = logging.StreamHandler()
    elif CONFIG['Log']['Output'] == 'file':
        now = datetime.datetime.now()
        # 添加按日期和文件大小的日志处理器
        log_handler = RotatingFileHandler(filename=f'{log_file_path}_{now.strftime("%Y-%m-%d")}.log',
                                          mode=CONFIG['Log']['AppendType'],
                                          encoding='utf-8',
                                          maxBytes=1024 * 1024 * CONFIG['Log']['FileSize'],
                                          backupCount=CONFIG['Log']['FileCount'])
    else:
        log_handler = logging.StreamHandler()  # 默认或填写错误时，输出到控制台
        root_logger.warning("日志输出类型配置错误或未配置，将默认输出到控制台")
    # 定义日志输出格式
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s-line%(lineno)d:%(message)s')
    # formatter = logging.Formatter('%(asctime)s %(name)s [%(levelname)s]:%(message)s')
    log_handler.setFormatter(formatter)

    # 给_logger添加handler
    root_logger.addHandler(log_handler)
    return root_logger


logger = init_logger()

if __name__ == "__main__":
    # 使用 logger 打印不同级别的日志
    # logger = init_logger()
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")
