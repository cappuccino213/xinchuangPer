"""
@File : kingbase_test.py
@Date : 2024/6/26 下午2:12
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import ksycopg2


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

    def execute(self, sql):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.commit()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    kb = KingBase("192.168.1.35", 54321, "system", "TomTaw@HZ", "imcis_per3")
    sql_statement = """SELECT count(1) from "dbo"."ExamRequest" """
    kb.execute(f"{sql_statement}")
