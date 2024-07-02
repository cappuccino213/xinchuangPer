"""
@File : main.py 程序主入口
@Date : 2024/7/1 下午1:25
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""

from log_config import logger
from db_drivers.dm import DM
from config import GLOBAL_CONFIG
from mock_data import MockData


def main():
    # 目标数据库
    da_meng = DM(GLOBAL_CONFIG["DestinationDataBase"]["db"]["user"],
                 GLOBAL_CONFIG["DestinationDataBase"]["db"]["password"],
                 GLOBAL_CONFIG["DestinationDataBase"]["db"]["host"],
                 GLOBAL_CONFIG["DestinationDataBase"]["db"]["port"])

    # 生成mock数据
    # 解析表结构
    columns = da_meng.get_table_structure('IMCIS', 'UserMst')

    data_to_insert = []
    # TODO 参数化处理：1、表名 2、生成数量 3、数据库驱动选择
    # TODO 带入配置文件的执行策略，加入多线程
    for row in range(5):
        # row_data = {}
        row_data = []
        for column in columns:
            generate_data = MockData.get_data_generator(column[0], column[1], column[2], column[3])
            # if generate_data:
                # row_data[column[0]] = generate_data
            row_data.append(generate_data)
            # logger.info(row_data)
        data_to_insert.append(row_data)

    # 批量插入数据
    logger.info(data_to_insert)
    # return data_to_insert

    # 拼接sql语句
    # field_list = [col[0] for col in columns if col[3].lower() in ("no", "n")]
    field_list = [col[0] for col in columns]
    sql_insert = 'INSERT INTO "IMCIS"."UserMst" ({0}) VALUES ({1})'.format(",".join(field_list), ",".join(["?" for _ in field_list]))
    logger.info(sql_insert)

    # 执行批量插入语句
    da_meng.execute_batch(sql_insert, data_to_insert)
    # logger.info(sql_insert)

if __name__ == '__main__':
    main()
