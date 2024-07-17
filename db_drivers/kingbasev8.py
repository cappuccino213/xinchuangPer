"""
@File : kingbasev8.py
@Date : 2024/7/9 上午10:21
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import ksycopg2

from db_drivers.database import DataBaseBase
from log_config import logger


class KingBaseV8(DataBaseBase):
    def __init__(self, host, port, user, password, dbname):
        super().__init__(host, port, user, password)
        self.dbname = dbname  # kingbasev8需要指定数据库名

    # 实现连接
    def connect(self):
        try:
            conn = ksycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                    dbname=self.dbname)
            return conn
        except Exception as e:
            logger.error(f"数据库连接失败：{e}")

    # 插入SQL拼接函数
    @staticmethod
    def concatenate_insert_sql(db_schema, table_name, column_list):
        """
        :param db_schema:数据库模式
        :param table_name: 表
        :param column_list: 表字段list
        :return: 插入的sql语句
        """
        # 列名需要加引号
        column_list = [f'"{column}"' for column in column_list]
        columns_str = ",".join(column_list)
        placeholder = ",".join(["%s"] * len(column_list))
        sql_statement = "INSERT INTO " + f'"{db_schema}"."{table_name}"' + f" ({columns_str}) " + f"VALUES ({placeholder})"
        logger.debug(sql_statement)
        return sql_statement

    # 获取表结构
    def get_table_structure(self, schema, table_name):
        sql = """SELECT column_name, 
        data_type, 
        character_maximum_length AS data_length,
        is_nullable
FROM information_schema.columns  
WHERE table_schema = %s  
AND table_name = %s
        """
        table_structure = self.execute_with_params(sql, (schema, table_name))
        logger.debug(f"获取到模式{schema}的表{table_name}的结构为：{table_structure}")
        return table_structure


if __name__ == "__main__":
    kb = KingBaseV8("192.168.1.35", 54321, "system", "TomTaw@HZ", "imcis_per3")
    kb.concatenate_insert_sql("dbo", "ExamRequest", ["ExamUID", "PatientID"])
    kb.get_table_structure("dbo", "ExamRequest")
