"""
@File : mock_data.py 数据模拟
@Date : 2024/7/2 下午2:30
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import string
import random
import uuid

import mimesis
from mimesis import Locale

from config import GLOBAL_CONFIG


# 生成数据
class MockData:
    # 加载mock初始化数据
    def __init__(self):
        self.config = GLOBAL_CONFIG.get("DataSource")
        self.varchar_max_length = self.config.get("mockup").get("varchar_max_length")
        self.int_max = self.config.get("mockup").get("int_max")

    # varchar生成器
    def generate_varchar(self, column_length):
        if column_length == 36:
            return str(uuid.uuid4()).upper()
        else:
            if column_length >= self.varchar_max_length:
                column_length = self.varchar_max_length
            return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(int(column_length)))

    # int生成器
    def generate_int(self):
        return random.randint(0, self.int_max)

    # 数据生成器
    def data_generator_simple(self, column_type, column_length):
        mimesis_gen = mimesis.Generic(Locale.ZH)
        data_generators = {
            "varchar": lambda: self.generate_varchar(column_length),
            # "varchar": lambda: self.generate_varchar(4),
            "bit": lambda: random.choice([0, 1]),
            "int": lambda: self.generate_int(),
            "integer": lambda: self.generate_int(),
            "smallint": lambda: self.generate_int(),
            "tinyint": lambda: random.choice(0, 1),
            "float": lambda: random.uniform(0, self.int_max),
            "double": lambda: random.uniform(0, self.int_max),
            "decimal": lambda: random.uniform(0, self.int_max),
            "numeric": lambda: random.uniform(0, self.int_max),
            "date": lambda: mimesis_gen.datetime.date().strftime("%Y-%m-%d"),
            "datetime": lambda: mimesis_gen.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "time": lambda: mimesis_gen.datetime.time().strftime("%H:%M:%S"),
            "timestamp": lambda: mimesis_gen.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S")
        }

        # 获取数据生成函数
        data_gen_func = data_generators.get(column_type.lower())

        return data_gen_func()

    # 业务模式数据生成
    def data_generator_business(self, column_type, column_length):
        mimesis_gen = mimesis.Generic(Locale.ZH)
        data_generators = {
            "varchar": lambda: self.generate_varchar(column_length),
            "bit": lambda: random.choice([0, 1]),
            "int": lambda: self.generate_int(),
            "integer": lambda: self.generate_int(),
            "smallint": lambda: self.generate_int(),
        }
        # TODO 完善业务模式数据生成


# 从数据库中获取数据
class SourceData:
    pass


if __name__ == "__main__":
    pass
