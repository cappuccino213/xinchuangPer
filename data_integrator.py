"""
@File : data_integrator.py
@Date : 2024/7/2 下午3:32
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import threading
from datetime import datetime
from datetime import timedelta

from config import GLOBAL_CONFIG
from db_drivers.dm8 import DM8
from db_drivers.kingbasev8 import KingBaseV8
from log_config import logger
from mock_data import MockData


# 数据库工厂类，提高可扩展性，之后有新增数据库直接修改此处即可
class DataBaseFactory:
    @staticmethod
    def create_database(driver_type: str, config: dict):
        if driver_type == "dm":
            return DM8(config["host"], config["port"], config["user"], config["password"])
        elif driver_type == "kingbase":
            return KingBaseV8(config["host"], config["port"], config["user"], config["password"], config["dbname"])
        else:
            logger.error(f"暂不支持的数据库类型：{driver_type}，如需添加，请联系作者")


# 数据集成类
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
    def data_gen_insert(self, db_table, row_amount) -> timedelta:
        """
        :param row_amount: 插入数据量
        :param db_table: 表名
        :return:返回的是执行时间
        """
        # 建立数据库连接
        driver_type = self.dst_db_config.get("driver")
        db_instance = DataBaseFactory.create_database(driver_type, self.dst_db_config)

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
        # 获取插入数据量
        # row_amount = self.task_config.get("DataSize")
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
        sql_statement = db_instance.concatenate_insert_sql(self.dst_db_schema, db_table, column_name_list)

        logger.debug(f"拼接插入表{db_table}的SQL语句成功：{sql_statement}")
        logger.info(f"插入表{db_table}的SQL语句拼接耗时：{datetime.now() - concatenate_start_time}")

        # 执行sql语句
        logger.info(f"[3]正在执行SQL语句...")
        result = db_instance.execute_batch(sql_statement, rows_data)
        if result:
            logger.info(f"执行SQL语句成功，表{db_table}共插入{len(rows_data)}条数据")
            return result

    # 执行插入任务

    # 根据分批次插入
    def insert_in_batches(self, table_name, batch_size, insert_amount):
        # 获取数据总量和批次数量
        """
        :param table_name: 表名称
        :param batch_size: 每批数量
        :param insert_amount: 插入总数量
        :return:
        """
        # 计算获取批数
        batch_num = int(insert_amount / batch_size)
        if batch_num > 0:
            """大于0执行分批插入"""
            logger.info(f"开始分批次插入，数据总量：{insert_amount}，每批数量：{batch_size}，共{batch_num}批")
            time_cost = timedelta()
            for i in range(batch_num):
                logger.info(f">>>>>>正在执行第{i + 1}批数据插入...")
                insert_time = self.data_gen_insert(table_name, batch_size)
                time_cost += insert_time
            logger.info(f"表{table_name}分{batch_num}批插入{insert_amount}条数据完成，共计耗时{time_cost}")
        else:
            """小于等于0执行单次插入"""
            logger.info(f"因数据总量{insert_amount}小于每批数量{batch_size}，不符合分批插入条件，执行单批插入...")
            time_cost = self.data_gen_insert(table_name, insert_amount)
            logger.info(f"表{table_name}插入{insert_amount}条数据完成，共计耗时{time_cost}")

    # 定义线程函数

    def thread_function(self, table_name):
        insert_amount = self.task_config.get("DataSize")
        batch_size = self.task_config.get("BatchSize")
        logger.info(f"表{table_name}数据开始插入-线程ID:{threading.get_ident()}")
        self.insert_in_batches(table_name, batch_size, insert_amount)

    def task_run(self):
        # 获取任务参数
        table_names = self.dst_db_tables
        # insert_amount = self.task_config.get("DataSize")
        # batch_size = self.task_config.get("BatchSize")
        thread_switch = self.task_config.get("IfMultiThread")

        # 是否启用多线程
        if thread_switch:
            threads = []
            for table_name in table_names:
                # 声明线程函数
                # thread_function = threading.Thread(target=self.insert_in_batches, args=(self.,))
                threads.append(threading.Thread(target=self.thread_function, args=(table_name,)))
            # 启动线程
            [t.start() for t in threads]

        # 普通模式
        else:
            for table in self.dst_db_tables:
                logger.info(f"任务开始执行...")
                self.data_gen_insert(table)


if __name__ == "__main__":
    di = DataIntegrator()
    # di.task_run()
    di.data_gen_insert("ExamRequest", 1000)
