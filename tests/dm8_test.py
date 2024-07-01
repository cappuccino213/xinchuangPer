"""
@File : dm8_test.py
@Date : 2024/6/26 下午4:52
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import dmPython
from log_config import logger

class DM:
    def __init__(self, user, password, server, port):
        self.user = user
        self.password = password
        self.server = server
        self.port = port

    def connect(self):
        conn = dmPython.connect(user=self.user, password=self.password, server=self.server, port=self.port)
        return conn

    def execute(self, sql):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        # for row in rows:
        #     print(row)
        conn.commit()
        cursor.close()
        conn.close()
        return rows




if __name__ == '__main__':
    dm = DM('SYSDBA', '123456789', '192.168.1.35', 5236)
    sql_statement = """SELECT top 1 * from IMCIS.ExamRequest"""
    sql_statement2 = """SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE  
FROM DBA_TAB_COLUMNS  
WHERE TABLE_NAME = 'UserMst' AND OWNER = 'IMCIS'
    """
    row = dm.execute(f'{sql_statement2}')
    # logger.info(row[0])
    for i in row[0]:
        logger.info(i)