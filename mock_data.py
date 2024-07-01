"""
@File : mock_data.py
@Date : 2024/6/28 上午11:24
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import random
# 模拟数据

import uuid
import mimesis
from mimesis.locales import Locale
from log_config import logger

mimesis_instance = mimesis.Generic(Locale.ZH)


class MockData:

    # UUID生成器
    # @staticmethod
    # def uuid_generator():
    #    return uuid.uuid4

    # 根据column_type类型和column_length选择数据的生成器
    @staticmethod
    def get_data_generator(column_name, column_type, column_length, column_is_nullable):
        # 判断字段值是否可为空，可为空则返回None
        # if column_is_nullable.lower() in ("yes", "y"):
        if 1 == 2 :
            return None
        # TODO 不能根据字段值为空判断是否生成数据，因为达梦的在迁移数据库时丢失了一些非空字段
        # 再根据字段类型和字段长度及字段名称生成数据
        else:
            if column_type.lower() == "bit":
                return random.choice((0, 1))
            if column_type.lower() == "tinyint":
                return random.choice((0, 1))
            if column_type.lower() in ("int", "integer"):
                if 'status' in column_name.lower():
                    return random.choice((0,1,2,4))
                elif 'complete' in column_name.lower():
                    return random.choice((0,1))
                else:
                    return random.randint(0, 100)
            if column_type.lower() == "bigint":
                return random.randint(10000000, 99999999)
            if column_type.lower() == "varchar":
                # 36长度的varchar，字段名带id或uuid的字段生成uuid
                if column_length == 36 and ("id" in column_name.lower() or "uid" in column_name.lower()):
                    return str(uuid.uuid4())
                # 64长度的varchar，字段名带id生成uuid，字段名含name的生成name
                if column_length == 64:
                    if "id" in column_name.lower():
                        return str(uuid.uuid4())
                    elif "name" in column_name.lower():
                        return mimesis_instance.person.name()
                    elif "account" in column_name.lower():
                        return mimesis_instance.person.username(mask='l')
                    # ToDo 完善其他varchar字段的生成
                    else:
                        return None
                    # return mimesis_instance.person.name


if __name__ == "__main__":
    logger.info(mimesis_instance.address.country())
    logger.info(mimesis_instance.person.name())
    # logger.info(str(MockData.uuid_generator()()).upper())
