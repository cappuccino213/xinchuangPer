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
from mimesis import Person

from config import GLOBAL_CONFIG

from log_config import logger


# 生成数据
class MockData:
    # 加载mock初始化数据
    def __init__(self):
        # 配置文件导入
        self.config = GLOBAL_CONFIG.get("DataSource")
        self.varchar_max_length = self.config.get("mockup").get("varchar_max_length")
        self.int_max = self.config.get("mockup").get("int_max")
        self.decimal_max = self.config.get("mockup").get("decimal_max")
        self.age_range = self.config.get("mockup").get("age")
        self.year_range = self.config.get("mockup").get("year")

        # 初始化mock实例
        self.person = Person(locale=Locale.ZH)
        self.generic = mimesis.Generic(locale=Locale.ZH)

    # 将字段名转换为小写
    @staticmethod
    def _lower_column_name(column_name):
        """为了避免重复调用.lower()，缓存结果"""
        if not isinstance(column_name, str):
            raise ValueError("column_name must be a string")
        return column_name.lower()

    """数据类型mock，_advanced表示带有一些业务逻辑的高级方法"""
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

    # text
    def generate_text(self):
        return self.generic.text.text()  # 默认是5个句子

    def generate_varchar_advanced(self, column_name, column_length, column_is_nullable):
        # 将字段名转换为小写
        lower_column_name = self._lower_column_name(column_name)
        # 检查字段是否可为空
        nullable_check = column_is_nullable.lower() in ("no", "n")

        # 特殊名称的字段处理
        specific_values = {
            "sex": random.choice(["男", "女"]),
            "ageunit": "岁",
            "organization": "哈尔滨市第二医院",
            "requestorg": "申请机构",
            "requestdept": "申请科室",
            # ... other specific mappings ...
            "patientclass": random.choice(["门诊", "住院", "急诊", "体检"]),
            "servicesectid": random.choice(["CT", "MR", "DR", "CR", "XA", "MG", "RF", "US"]),
            "organizationid": "QWYHZYFAZX",
            "account": self.person.username(mask="l"),
            "patientmasterid": str(uuid.uuid4()),
            "businessid": str(uuid.uuid4()),
            "visitid": str(uuid.uuid4()),
            # ... rest of the specific cases ...
        }
        if lower_column_name in specific_values:
            return specific_values[lower_column_name]

        # Special handling for UUID-like fields
        elif lower_column_name.endswith("uid") and lower_column_name.startswith(
                ("service", "media", "resultservice", "study")):
            return "00000000-0000-0000-0000-000000000000"
        elif lower_column_name.endswith("uid"):
            return str(uuid.uuid4()).upper()

        elif nullable_check and column_length > 0:
            return "".join(random.choice(string.ascii_letters + string.digits) for _ in
                           range(min(int(column_length), self.int_max)))  # 避免小数时的长度不是整数
        else:
            return ""

    def generate_bit_advanced(self, column_name):
        lower_column_name = self._lower_column_name(column_name)
        if lower_column_name.endswith("flag"):
            return 0
        elif lower_column_name == "age":
            return random.randint(self.age_range[0], self.age_range[1])
        else:
            return random.choice([0, 1])

    def generate_int_advanced(self, column_name):
        # 标记、状态类型
        lower_column_name = self._lower_column_name(column_name)
        if lower_column_name.endswith(("flag", "status", "state", "need", "count")):
            return random.choice([0, 1])
        elif lower_column_name in ["patientid", "accessionnumber", "medrecno"]:
            return random.randint(0, self.int_max)
        else:
            return 0

    def generate_decimal_advanced(self, column_type):
        lower_column_type = self._lower_column_name(column_type)
        if lower_column_type in ("decimal", 'numeric', "float", "double"):
            return random.randint(0, self.decimal_max)
        else:
            return 0.0

    def generate_time_advanced(self, column_type):
        lower_column_type = self._lower_column_name(column_type)
        if lower_column_type in ["datetime", "timestamp"]:
            return self.generic.datetime.datetime(start=self.year_range[0], end=self.year_range[1]).strftime(
                "%Y-%m-%d %H:%M:%S")
        elif lower_column_type == "date":
            return self.generic.datetime.date(start=self.year_range[0], end=self.year_range[1]).strftime("%Y-%m-%d")
        elif lower_column_type == "time":
            return self.generic.datetime.time().strftime("%H:%M:%S")
        else:
            return ""

    """数据行生成器"""
    def data_generator_simple(self, column_type, column_length):
        data_generators = {
            "varchar": lambda: self.generate_varchar(column_length),
            "bit": lambda: random.choice([0, 1]),
            "int": lambda: self.generate_int(),
            "integer": lambda: self.generate_int(),
            "smallint": lambda: self.generate_int(),
            "tinyint": lambda: random.choice([0, 1]),
            "numeric": lambda: self.generate_decimal_advanced(column_type),
            "decimal": lambda: self.generate_decimal_advanced(column_type),
            "double": lambda: self.generate_decimal_advanced(column_type),
            "float": lambda: self.generate_decimal_advanced(column_type),
            "date": lambda: self.generic.datetime.date().strftime("%Y-%m-%d"),
            "datetime": lambda: self.generic.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "time": lambda: self.generic.datetime.time().strftime("%H:%M:%S"),
            "timestamp": lambda: self.generic.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp without time zone": lambda: self.generate_time_advanced("timestamp"),
            "text": lambda: self.generate_text()
        }
        # 获取数据生成函数
        data_gen_func = data_generators.get(column_type.lower())
        return data_gen_func()

    # 业务模式数据生成
    def data_generator_business(self, column_name, column_type, column_length, column_is_nullable):
        data_generators = {
            "varchar": lambda: self.generate_varchar_advanced(column_name, column_length, column_is_nullable),
            "bit": lambda: self.generate_bit_advanced(column_name),
            "int": lambda: self.generate_int_advanced(column_name),
            "integer": lambda: self.generate_int_advanced(column_name),
            "smallint": lambda: self.generate_int_advanced(column_name),
            "numeric": lambda: self.generate_decimal_advanced(column_type),
            "decimal": lambda: self.generate_decimal_advanced(column_type),
            "double": lambda: self.generate_decimal_advanced(column_type),
            "float": lambda: self.generate_decimal_advanced(column_type),
            "date": lambda: self.generate_time_advanced("date"),
            "datetime": lambda: self.generate_time_advanced("datetime"),
            "time": lambda: self.generate_time_advanced("time"),
            "timestamp": lambda: self.generate_time_advanced("timestamp"),
            "timestamp without time zone": lambda: self.generate_time_advanced("timestamp"),
            "text": lambda: self.generate_text()
        }

        data_gen_func = data_generators.get(column_type.lower())
        if data_gen_func:
            return data_gen_func()
        else:
            logger.warn(f"未找到类型为{column_type.lower()}的生成函数，返回空字符")
        return ""


# 从数据库中获取数据
class SourceData:
    pass


if __name__ == "__main__":
    md = MockData()
    logger.info(md.generate_text())
