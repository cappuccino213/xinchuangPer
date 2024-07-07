"""
@File : kingbase.py 人大金仓数据驱动
@Date : 2024/6/27 下午4:26
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
from datetime import datetime

import ksycopg2

from log_config import logger


class KingBase:
    def __init__(self, host, port, user, password, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname

    def connect(self):
        return ksycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                dbname=self.dbname)

    # 带参数执行sql
    def execute_with_params(self, sql, params=None):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    # 拼接插入sql语句
    @staticmethod
    def concatenate_insert_sql(db_schema, table_name, column_list):
        # 列名需要加引号
        column_list = [f'"{column}"' for column in column_list]
        columns_str = ",".join(column_list)
        placeholder = ",".join(["%s"] * len(column_list))
        # sql_statement = f"INSERT INTO {db_schema}.{table_name} ({columns_str}) VALUES ({placeholder})"
        sql_statement = "INSERT INTO "+f'"{db_schema}"."{table_name}"'+ f" ({columns_str}) " + f"VALUES ({placeholder})"
        logger.debug(sql_statement)
        return sql_statement

    def execute_batch(self, sql, sequence_of_params):
        # 执行耗时计算
        execute_begin_time = datetime.now()
        # 确保参数非空，以处理边界条件
        if not sequence_of_params:
            logger.warning("sequence_of_params为空，不执行任何操作")
            return []
        try:
            # 用with语句前处理上下文，防止忘记关闭连接
            with self.connect() as connect:
                with connect.cursor() as cursor:
                    cursor.executemany(sql, sequence_of_params)
                    connect.commit()
            logger.info(f"【执行sql语句成功，耗时：{datetime.now() - execute_begin_time}】")
            return True
        except Exception as e:
            logger.error(f"执行sql语句失败，错误信息：{e}")
            raise e

    # 获取表结构
    def get_table_structure(self, table_schema, table_name):
        sql = """SELECT column_name, 
        data_type, 
        character_maximum_length AS data_length,
        is_nullable
FROM information_schema.columns  
WHERE table_schema = %s  
AND table_name = %s
        """
        columns = self.execute_with_params(sql, (table_schema, table_name))
        logger.debug(columns)
        return columns


if __name__ == "__main__":
    kb = KingBase("192.168.1.35", 54321, "system", "TomTaw@HZ", "imcis_per3")
    # kb.get_table_structure('dbo', 'UserMst')
    kb.concatenate_insert_sql("dbo", "ExamRequest", ["ExamUID", "PatientID"])
