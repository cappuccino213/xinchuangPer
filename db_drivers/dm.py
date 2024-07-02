"""
@File : dm.py 达梦数据驱动
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
        # 确保参数非空，以处理边界条件
        if not sequence_of_params:
            logger.warning("sequence_of_params为空，不执行任何操作")
            return []
        try:
            # 用with语句前处理上下文，防止忘记关闭连接
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(sql, sequence_of_params)
                    rows = cursor.fetchall
                    conn.commit()
            return rows
        except Exception as e:
            logger.error(f"执行sql语句失败，错误信息：{e}")
            raise e

    # 分析表结构
    def get_table_structure(self, schema, table_name):
        """
        :param schema: 模式
        :param table_name: 表名
        :return: [字段名，字段类型，字段长度，是否为空] eg：[('UserUID', 'VARCHAR', Decimal('36'), 'N'), ('Account', 'VARCHAR', Decimal('64'), 'N')]
        """
        sql_statement = """SELECT column_name, 
                           data_type, 
                           data_length, 
                           nullable AS is_nullable
                        FROM DBA_TAB_COLUMNS
                        WHERE TABLE_NAME = (?) 
                          AND OWNER = (?)"""
        columns = self.execute_with_params(sql_statement, (table_name, schema))
        # logger.debug(columns)
        return columns


if __name__ == "__main__":
    dm = DM('SYSDBA', '123456789', '192.168.1.35', 5236)
    dm.get_table_structure('IMCIS', 'UserMst')
