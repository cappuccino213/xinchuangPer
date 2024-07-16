"""
@File : DataBaseBase.py
@Date : 2024/7/8 下午5:02
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
from datetime import datetime
from log_config import logger

"""数据库基类"""


class DataBaseBase:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    # 抽象方法，由子类实现
    def connect(self):
        raise NotImplementedError("子类必须实现该方法")

    # 执行SQL语句，带参数
    def execute_with_params(self, sql, params=None):
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            # 使用参数化查询避免SQL注入攻击
            cursor.execute(sql, params or ())
            conn.commit()
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"数据库操作失败，错误信息：{e}")
        # 关闭游标和连接
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # 带参数的批量插入
    def execute_batch(self, sql, sequence_of_params):
        """
        :param sql:
        :param sequence_of_params:
        :return: 返回执行的耗时
        """
        # 记录开始时间
        execute_start_time = datetime.now()
        # 确保参数非空，以处理边界条件
        if not sequence_of_params:
            logger.error("sequence_of_params为空，不执行任何操作")
            return []
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            # 使用参数化查询避免SQL注入攻击
            cursor.executemany(sql, sequence_of_params)
            conn.commit()
            operate_time = datetime.now() - execute_start_time
            logger.info(f"【数据库executemany操作成功，耗时：{operate_time}】")
            return operate_time
        except Exception as e:
            conn.rollback()
            logger.error(f"数据库操作失败，错误信息：{e}")
        # 关闭连接
        finally:
            if conn:
                conn.close()


if __name__ == "__main__":
    pass
