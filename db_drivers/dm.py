"""
@File : dm.py
@Date : 2024/6/27 下午4:25
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
from log_config import logger

# 达梦数据库连接，不同的python版本对应不同的驱动
# 详见https://eco.dameng.com/document/dm/zh-cn/app-dev/python-python.html
# 当前项目是在dm8的drivers中python3.10.4下编译的

import dmPython


class DM:
    def __init__(self, user, password, host, port):
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    # 建立连接
    def connect(self):
        conn = dmPython.connect(user=self.user, password=self.password, host=self.host, port=self.port)
        return conn

    # 执行sql语句
    def execute(self, sql):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        return rows

    # 带参数执行sql语句
    def execute_with_params(self, sql, params=None):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        return rows

    # 批量绑定参数执行
    def execute_batch(self, sql, sequence_of_params):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.executemany(sql, sequence_of_params)
        # rows = cursor.fetchall()
        conn.commit()
        cursor.close()
        conn.close()
        # return rows
        # TODO 去掉多余的语句

    # 分析表结构
    def get_table_structure(self, owner, table_name):
        sql_statement = """SELECT column_name, 
                           data_type, 
                           data_length, 
                           nullable AS is_nullable
                        FROM DBA_TAB_COLUMNS
                        WHERE TABLE_NAME = (?) 
                          AND OWNER = (?)"""
        columns = self.execute_with_params(sql_statement, (table_name, owner))
        logger.info(columns)
        return columns


if __name__ == "__main__":
    dm = DM('SYSDBA', '123456789', '192.168.1.35', 5236)
    dm.get_table_structure('IMCIS', 'UserMst')
