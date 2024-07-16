"""
@File : dm8.py
@Date : 2024/7/8 下午5:19
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import dmPython
from log_config import logger

from db_drivers.database import DataBaseBase

# 达梦数据库连接，不同的python版本对应不同的驱动
# 详见https://eco.dameng.com/document/dm/zh-cn/app-dev/python-python.html
# 当前项目是在dm8的drivers中python3.10.4下编译的


class DM8(DataBaseBase):
    def __init__(self, host, port, user, password):
        super().__init__(host, port, user, password)

    # 子类实现连接函数
    def connect(self):
        try:
            conn = dmPython.connect(host=self.host, port=self.port, user=self.user, password=self.password)
            return conn
        except Exception as e:
            logger.error(f"数据库连接失败：{e}")

    # def dm8_execute_with_params(self, sql, params=None):
    #     super().execute_with_params(sql, params)

    # 插入SQL拼接函数
    @staticmethod
    def concatenate_insert_sql(db_schema, table_name, column_list):
        """
        :param db_schema:数据库模式
        :param table_name: 表
        :param column_list: 表字段list
        :return: 插入的sql语句
        """
        column_str = ','.join(column_list)
        placeholders = ','.join(['?'] * len(column_list))
        sql_statement = f"INSERT INTO {db_schema}.{table_name} ({column_str}) VALUES ({placeholders})"
        logger.debug(sql_statement)
        return sql_statement

    # 获取表结构
    def get_table_structure(self, schema, table_name):
        """
        :param schema: 数据库模式
        :param table_name: 表名
        :return:  [字段名，字段类型，字段长度，是否为空]
        """
        sql_statement = """SELECT column_name, 
                           data_type, 
                           data_length, 
                           nullable AS is_nullable
                        FROM DBA_TAB_COLUMNS
                        WHERE TABLE_NAME = (?) 
                          AND OWNER = (?)"""
        table_structure = self.execute_with_params(sql_statement, params=(table_name, schema))
        logger.debug(f"获取到模式{schema}的表{table_name}的结构为：{table_structure}")
        return table_structure


if __name__ == "__main__":
    dm = DM8('192.168.1.35', 5236,'SYSDBA', '123456789',)
    # # dm.get_table_structure('IMCIS', 'UserMst')
    # dm.concatenate_insert_sql('IMCIS', 'ExamRequest', ["ExamUID", "PatientID"])
    dm.get_table_structure('IMCIS', 'ExamRequest')
