"""
@File : kingbase.py 人大金仓数据驱动
@Date : 2024/6/27 下午4:26
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
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
    # 获取表结构
    def get_table_structure(self,table_schema,table_name):
        sql = """SELECT column_name, data_type, character_maximum_length,is_nullable
FROM information_schema.columns  
WHERE table_schema = %s  
AND table_name = %s
        """
        columns = self.execute_with_params(sql,(table_schema,table_name))
        logger.info(columns)
        return columns


if __name__ == "__main__":
    kb = KingBase("192.168.1.35", 54321, "system", "TomTaw@HZ", "imcis_per3")
    kb.get_table_structure('dbo','UserMst')
