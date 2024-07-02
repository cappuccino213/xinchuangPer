"""
@File : data_integrator.py
@Date : 2024/7/2 下午3:32
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
from config import GLOBAL_CONFIG
from db_drivers.dm import DM
from log_config import logger
from mock_data import MockData


class DataIntegrator:
    def __init__(self):
        self.data_src_config = GLOBAL_CONFIG.get("DataSource")
        self.dst_db_config = GLOBAL_CONFIG.get("DestinationDataBase")
        self.task_config = GLOBAL_CONFIG.get("Task")

    def data_insert_dm(self):
        # 创建数据库实例
        dm = DM(self.dst_db_config.get("db").get("user"),
                self.dst_db_config.get("db").get("password"),
                self.dst_db_config.get("db").get("host"),
                self.dst_db_config.get("db").get("port"))
        # 解析表结构
        db_schema = self.dst_db_config.get("db").get("dbname")
        db_tables = self.dst_db_config.get("db").get("tables")
        table_structures = [dm.get_table_structure(db_schema, table) for table in db_tables]
        # logger.debug(table_structures)
        # 生成指定数量
        # 根据表把生成数据分组
        rows_data_group = []
        rows_amount = self.task_config.get("DataSize")
        for ts in table_structures:
            # 数据记录行
            rows_data = []
            for row in range(rows_amount):
                cols_data = []  # 字段列
                # 根据生成模式
                gen_mode = self.data_src_config.get("mockup").get("model")
                # 生成数据
                for col in ts:
                    if gen_mode == "simple":
                        cols_data.append(MockData().data_generator_simple(col[1], col[2]))
                    else:
                        # TODO 逻辑需要完善，根据字段名、类型生成
                        cols_data.append(MockData().data_generator_business(col[1], col[2]))
                rows_data.append(cols_data)
            rows_data_group.append(rows_data)
        logger.debug(rows_data_group)

        # 执行插入语句
        for table in db_tables:
            pass
        # TODO 考虑多线程插入


    def data_insert_kingbase(self):
        pass


if __name__ == "__main__":
    DataIntegrator().data_insert_dm()
