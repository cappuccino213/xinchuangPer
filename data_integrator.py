"""
@File : data_integrator.py
@Date : 2024/7/2 下午3:32
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import threading
from datetime import datetime

from config import GLOBAL_CONFIG
from db_drivers.dm import DM
from db_drivers.kingbase import KingBase
from log_config import logger
from mock_data import MockData


class DataIntegrator:
    def __init__(self):
        # 数据源配置
        self.data_src_config = GLOBAL_CONFIG.get("DataSource")
        # 目标数据库配置
        self.dst_db_config = GLOBAL_CONFIG.get("DestinationDataBase").get("db")
        self.dst_db_schema = self.dst_db_config.get("dbschema")
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
        driver_type = self.dst_db_config.get("driver")
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
        def cols_data_generator(table_structure, mockup_instance):
            """
            :db_type:数据库类型
            :table_structure: 表结构
            :return: 生成数据行，待插入的数据列名
            """
            cols_data = []  # 列数据（字段值）
            fields_name = []
            gen_mode = self.data_src_config.get("mockup").get("model")
            # mock_data_instance = MockData()
            for col in table_structure:
                if gen_mode == "simple":
                    if col[3].lower() in ("n", "no"):  # 只添加不为空的字段
                        # col_value = MockData().data_generator_simple(col[1], col[2])
                        col_value = mockup_instance.data_generator_simple(col[1], col[2])
                        # 若生成的数据不为空字符时，则添加到列数据中,以保持列和值的数量匹配
                        if col_value != "":
                            cols_data.append(col_value)
                            fields_name.append(col[0])
                else:
                    col_value = mockup_instance.data_generator_business(col[0], col[1], col[2], col[3])
                    # 若生成的数据不为空，则添加到列数据中
                    if col_value != "":
                        cols_data.append(col_value)
                        fields_name.append(col[0])
            return cols_data, fields_name

        # 生成指定数量的数据行，和数据列名
        rows_data = []  # 行数据
        row_amount = self.task_config.get("DataSize")
        # 创建一个mock实例
        mock_data_instance = MockData()
        # 耗时计算
        generate_start_time = datetime.now()
        logger.info(f"[1]表{db_table}正在生成{row_amount}条数据...")
        # 数据列名
        column_name_list = []
        for row in range(row_amount):
            logger.debug(f"正在生成第{row}条数据...")
            # 分别获取数据行，获取数据列名（每行都一样，获取一次即可）
            gen_col_data = cols_data_generator(cols, mock_data_instance)
            col_data = gen_col_data[0]
            if len(column_name_list) == 0:
                column_name_list = gen_col_data[1]
            logger.debug(f"表{db_table}-第{row}条数据：{gen_col_data}")
            rows_data.append(col_data)
        logger.info(f"表{db_table}生成数据成功，共生成{len(rows_data)}条数据")
        logger.info(f"表{db_table}生成数据耗时：{datetime.now() - generate_start_time}")

        # 拼接sql语句
        # 耗时计算
        concatenate_start_time = datetime.now()
        logger.info(f"[2]正在拼接插入表{db_table}的SQL语句...")
        # fields = [col[0] for col in cols if col[3].lower() in ("n", "no")] # 简单模式用
        """拼接插入SQL语句函数"""
        # columns_str = ",".join(column_name_list)
        # placeholder = ",".join(["?" for _ in range(len(column_name_list))])
        # sql_statement = "INSERT INTO " + self.dst_db_schema + "." + db_table + f" ({columns_str}) " + " VALUES " + f"({placeholder})"
        sql_statement = db_instance.concatenate_insert_sql(self.dst_db_schema, db_table, column_name_list)

        logger.debug(f"拼接插入表{db_table}的SQL语句成功：{sql_statement}")
        logger.info(f"插入表{db_table}的SQL语句拼接耗时：{datetime.now() - concatenate_start_time}")

        # 执行sql语句
        logger.info(f"[3]正在执行SQL语句...")
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
    # di.task_run()
    di.data_gen_insert("ExamRequest")
