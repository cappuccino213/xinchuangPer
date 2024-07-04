"""
@File : data_integrator.py
@Date : 2024/7/2 下午3:32
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import threading

from config import GLOBAL_CONFIG
from db_drivers.dm import DM
from db_drivers.kingbase import KingBase
from log_config import logger
from mock_data import MockData


class DataIntegrator:
    def __init__(self):
        # 获取配置
        # 数据原配置
        self.data_src_config = GLOBAL_CONFIG.get("DataSource")
        # 目标数据库配置
        self.dst_db_config = GLOBAL_CONFIG.get("DestinationDataBase").get("db")
        self.dst_db_schema = self.dst_db_config.get("dbname")
        self.dst_db_tables = self.dst_db_config.get("tables")
        # 任务配置
        self.task_config = GLOBAL_CONFIG.get("Task")

    # 生成数据
    def data_gen_insert(self, db_table):
        """
        :param db_table: 表名
        :return:
        """
        # 建立数据库连接
        driver_type = self.dst_db_config.get("driver_type")
        if driver_type == "dm":
            db_instance = DM(self.dst_db_config.get("user"),
                             self.dst_db_config.get("password"),
                             self.dst_db_config.get("host"),
                             self.dst_db_config.get("port"))
        elif driver_type == "kingbase":
            db_instance = KingBase(self.dst_db_config.get("host"),
                                   self.dst_db_config.get("port"),
                                   self.dst_db_config.get("user"),
                                   self.dst_db_config.get("password"),
                                   self.dst_db_config.get("dbname"))
        else:  # TODO 替换成工厂模式
            db_instance = DM(self.dst_db_config.get("user"),
                             self.dst_db_config.get("password"),
                             self.dst_db_config.get("host"),
                             self.dst_db_config.get("port"))

        # 解析表字段
        cols = db_instance.get_table_structure(self.dst_db_schema, db_table)

        # 生成单条数据函数
        def cols_data_generator():
            cols_data = []  # 列数据（字段值）
            fields_data = []
            for col in cols:
                gen_mode = self.data_src_config.get("mockup").get("model")
                if gen_mode == "simple":
                    if col[3].lower() in ("n", "no"):  # 只添加不为空的字段
                        col_data = MockData().data_generator_simple(col[1], col[2])
                        # 若生成的数据不为空，则添加到列数据中
                        if col_data != "":
                            cols_data.append(col_data)
                            fields_data.append(col[0])
                else:
                    col_data = MockData().data_generator_business(col[0], col[1], col[2], col[3])
                    # 若生成的数据不为空，则添加到列数据中
                    if col_data != "":
                        cols_data.append(col_data)
                        fields_data.append(col[0])
            return cols_data, fields_data

        # 生成指定数量
        rows_data = []  # 行数据 （每一条数据表记录）
        row_amount = self.task_config.get("DataSize")
        logger.info(f"1、表{db_table}正在生成{row_amount}条数据...")
        for row in range(row_amount):
            logger.debug(f"正在生成第{row}条数据...")
            gen_col_data = cols_data_generator()[0]
            logger.debug(f"第{row}条数据：{gen_col_data}")
            rows_data.append(gen_col_data)
        logger.info(f"生成数据成功，共生成{len(rows_data)}条数据")

        # 拼接sql语句
        logger.info(f"2、正在拼接SQL语句...")
        # fields = [col[0] for col in cols if col[3].lower() in ("n", "no")] # 简单模式用
        # 根据行数据中值不为空的字段名拼成字段列
        fields = cols_data_generator()[1]

        fields_str = ",".join(fields)
        placeholder = ",".join(["?" for _ in range(len(fields))])
        sql_statement = "INSERT INTO " + self.dst_db_schema + "." + db_table + f" ({fields_str}) " + " VALUES " + f"({placeholder})"
        logger.debug(f"生成SQL语句成功：{sql_statement}")

        # 执行sql语句
        logger.info(f"3、正在执行SQL语句...")
        result = db_instance.execute_batch(sql_statement, rows_data)
        if result:
            logger.info(f"执行SQL语句成功，表{db_table}共插入{len(rows_data)}条数据")

    # 执行插入任务
    def task_run(self):
        # 是否启用多线程
        if self.task_config.get("IfMultiThread"):
            threads = []
            for table in self.dst_db_tables:
                threads.append(threading.Thread(target=self.thread_function, args=(table,)))
            # 启动线程
            [t.start() for t in threads]

        # 普通模式
        else:
            for table in self.dst_db_tables:
                logger.info(f"任务开始执行...")
                self.data_gen_insert(table)

    # 线程id打印函数
    def thread_function(self, table_name):
        logger.info(f"表{table_name}数据开始插入-线程ID:{threading.get_ident()}")
        self.data_gen_insert(table_name)


if __name__ == "__main__":
    di = DataIntegrator()
    di.task_run()
