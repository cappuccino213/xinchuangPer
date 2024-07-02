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
from config import GLOBAL_CONFIG

from string import ascii_lowercase

mimesis_instance = mimesis.Generic(Locale.ZH)


class MockData:

    @staticmethod
    def get_data_generator(column_name=None, column_type=None, column_length=None, column_is_nullable=None):

        # 使用get取数值，防止默认值不正确引发的错误
        if GLOBAL_CONFIG.get("DataSource").get("mockup").get("model") == "simple":
            if column_is_nullable.lower() in ("no", "n"):
                # 根据数据类型和长度生成数据，持续补充
                # 字符数据
                if column_type.lower() == "varchar":
                    # 限制长度，防止字符串过长生成的数据很大，影响性能
                    if column_length >= 128:
                        column_length = 128
                    return ''.join(random.choice(ascii_lowercase) for _ in range(int(column_length)))
                # uuid数据
                elif column_type.lower() == "uniqueidentifier":
                    return uuid.uuid4()
                # 位串数据
                elif column_type.lower() == "bit":
                    return random.choice((0, 1))
                # 数值数据（精确）NUMBER INT INTEGER SMALLINT BIGINT等
                elif column_type.lower() in ("int", "integer", "smallint", "tinyint","bigint"):
                    if column_type.lower() == "tinyint":
                        return random.choice((0, 1))
                    else:
                        return random.randint(0, 10000000)
                # 数字数据（近似）FLOAT DOUBLE等
                elif column_type.lower() in ("float", "double"):
                    return random.uniform(0, 10000000)
                # 日期时间类型
                elif column_type.lower() in ("date", "datetime", "datetime2", "datetimeoffset", "smalldatetime",
                                              "time", "timestamp"):
                    if column_type.lower() == "date":
                        return mimesis_instance.Datetime().date()
                    elif column_type.lower() == "time":
                        return mimesis_instance.Datetime().time()
                    elif column_type.lower() == "timestamp":
                        return mimesis_instance.Datetime().datetime()
                    else:
                        return mimesis_instance.Datetime().datetime()
                # 其他类型
                else:
                    logger.warning("不支持的数据类型：" + column_type)
                    return None

        # 完整的数据模拟
        elif GLOBAL_CONFIG.get("DataSource").get("mockup").get("model") == "complete":
            pass
        else:
            raise Exception("不支持的模拟数据模式：" + GLOBAL_CONFIG.get("DataSource").get("mockup").get("model"))
            # if column_type.lower() == "bit":



            #     return random.choice((0, 1))
            # if column_type.lower() == "tinyint":
            #     return random.choice((0, 1))
            # if column_type.lower() in ("int", "integer"):
            #     if 'status' in column_name.lower():
            #         return random.choice((0, 1, 2, 4))
            #     elif 'complete' in column_name.lower():
            #         return random.choice((0, 1))
            #     else:
            #         return random.randint(0, 100)
            # if column_type.lower() == "bigint":
            #     return random.randint(10000000, 99999999)
            # if column_type.lower() == "varchar":
            #     # 36长度的varchar，字段名带id或uuid的字段生成uuid
            #     if column_length == 36 and ("id" in column_name.lower() or "uid" in column_name.lower()):
            #         return str(uuid.uuid4())
            #     # 64长度的varchar，字段名带id生成uuid，字段名含name的生成name
            #     if column_length == 64:
            #         if "id" in column_name.lower():
            #             return str(uuid.uuid4())
            #         elif "name" in column_name.lower():
            #             return mimesis_instance.person.name()
            #         elif "account" in column_name.lower():
            #             return mimesis_instance.person.username(mask='l')
            # # 其他情况，当字段允许为空时，返回None
            #     return None


if __name__ == "__main__":
    md = MockData().get_data_generator()
