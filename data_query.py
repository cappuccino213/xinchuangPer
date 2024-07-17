"""
@File : data_query.py
@Date : 2024/7/16 下午3:51
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""

from data_integrator import DataBaseFactory
from config import GLOBAL_CONFIG
from log_config import logger


class DataQuery:
    def __init__(self):
        self.dst_db_config = GLOBAL_CONFIG.get("DestinationDataBase").get("db")
        self.dst_db_schema = self.dst_db_config.get("dbschema")
        self.dst_db_tables = self.dst_db_config.get("tables")
        self.task_config = GLOBAL_CONFIG.get("Task")
        self.test_cases = GLOBAL_CONFIG.get("test_cases")

    # 执行一组查询语句，同时返回统计查询耗时
    def query_all(self, queries: list[tuple], query_num=3):
        """
        1.执行所有sql语句进行查询
        2.按照设定的查询次数，统计每次查询耗时的平均值（查询次数大等于3，否则取最小值）
        3.以列表的形式返回所有查询语句的耗时，列表元素为tuple类型第一元素为查询名称，第二个元素为耗时
        :param query_num: 查询次数,默认查询3次
        :param queries: [("查询1"："sql1"),("查询2"："sql2")]
        :return: [("查询1", "耗时1"), ("查询2", "耗时2")]
        """
        db_instance = DataBaseFactory.create_database(self.dst_db_config.get("driver"), self.dst_db_config)
        if queries not in [None, []]:
            query_result = []
            for query in queries:
                time_cost_list = []
                for i in range(query_num):
                    result, time_cost = db_instance.execute(query[1])
                    # 将timedelta转换为秒
                    time_cost = time_cost.total_seconds()
                    logger.info(f"第{i + 1}次执行{query[0]}查询，耗时{time_cost}秒")
                    time_cost_list.append(time_cost)
                if query_num >= 3:
                    get_time_cost = sum(time_cost_list) / len(time_cost_list)
                else:
                    get_time_cost = min(time_cost_list)
                    # 结果保留3位小数
                query_result.append((query[0], round(get_time_cost, 3)))
            logger.debug(query_result)
            return query_result
        else:
            logger.error("查询语句为空，不执行查询")

    # 数据库查询性能
    def query_per_test(self,test_db_type):
        # 根据数据库类型，获取对应的数据库执行语句标识
        db_type_map = {
            "kingbase": "kingbase_query",
            "dm": "dm_query",
            "oracle": "oracle_query",
            "sqlserver": "mssql_query",
            "mysql": "mysql_query"
            # 按需扩充，配置文件中的测试用例也需要扩充
        }

        # 获取测试用例
        test_case_list = self.test_cases.get("query_per_test")

        # 格式化测试用例，使其符合query_all的执行参数
        query_list = []
        for test_case in test_case_list:
            query_list.append((test_case.get("stage_name"), test_case.get(db_type_map[test_db_type])))

        # 执行测试
        test_result = self.query_all(query_list, query_num=3)
        logger.info(test_result)
        return test_result


if __name__ == "__main__":
    dq = DataQuery()
    # dq.query_all([(query_name, sql_t)], 4)
    # dq.query_per_test("dm")
    dq.query_per_test("kingbase")
