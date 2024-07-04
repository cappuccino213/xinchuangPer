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
        self.config = GLOBAL_CONFIG.get("DataSource")
        self.varchar_max_length = self.config.get("mockup").get("varchar_max_length")
        self.int_max = self.config.get("mockup").get("int_max")
        self.decimal_max = self.config.get("mockup").get("decimal_max")
        self.age_range = self.config.get("mockup").get("age")
        self.year_range = self.config.get("mockup").get("year")

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

    # 增加业务逻辑的高级生成器，可以根据实际目标数据库的需求，进行扩展
    def generate_varchar_advanced(self, column_name, column_length, column_is_nullable):
        mimesis_gen = mimesis.Generic(Locale.ZH)
        mimesis_person = Person(Locale.ZH)
        if column_name.lower() == "sex":
            return random.choice(["男", "女"])
        elif column_name.lower == "ageunit":
            return "岁"
        elif column_name.lower().endswith("name"):
            if column_name.lower().startswith("organization"):
                return "哈尔滨市第二医院"
            elif column_name.lower().startswith("requestorg"):
                return "申请机构"
            elif column_name.lower().startswith("requestdept"):
                return "申请科室"
            else:
                # return mimesis_gen.person.full_name()
                return mimesis_gen.person.last_name() + mimesis_gen.person.first_name()
        elif column_name.lower().endswith("uid"):
            if column_name.lower().startswith(("service", "media", "resultservice", "study")):
                return "00000000-0000-0000-0000-000000000000"
            else:
                return str(uuid.uuid4()).upper()
        elif column_name.lower() in ["patientmasterid", "businessid", "visitid"]:
            return str(uuid.uuid4())
        elif column_name.lower() == "patientclass":
            return random.choice(["门诊", "住院", "急诊", "体检"])
        elif column_name.lower() == "servicesectid":
            return random.choice(["CT", "MR", "DR", "CR", "XA", "MG", "RF", "US"])
        elif column_name.lower() == "organizationid":
            return "QWYHZYFAZX"
        elif column_name.lower() == "account":
            return mimesis_gen.person.username(mask="l")
        elif column_name.lower() in ["patientid", "accessionnumber", "medrecno"]:
            return random.randint(0, self.int_max)
        elif column_name.lower() in ("birthday", "birthdate", "birth"):
            return mimesis_person.birthdate(min_year=1970, max_year=2023)
        elif column_name.lower() == "resultstatuscode":
            return random.choice(["3050", "3080", "3090"])
        elif column_name.lower() in ["businesstype", "classcode"]:
            return "Exam"
        elif column_name.lower() == "typecode":
            return "ExamFilm"
        # 如果匹配不到以上规则字段，根据该字段是否可为空，空则返回空字符串
        elif column_is_nullable.lower() in ("no", "n"):
            if column_length >= self.varchar_max_length:
                column_length = self.varchar_max_length
            return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(int(column_length)))
        else:
            return ""

    def generate_bit_advanced(self, column_name):
        if column_name.lower().endswith("flag"):
            return 0
        elif column_name.lower() == "age":
            return random.randint(self.age_range[0], self.age_range[1])
        else:
            return random.choice([0, 1])

    def generate_int_advanced(self, column_name):
        # 标记、状态类型
        if column_name.lower().endswith(("flag", "status", "state", "need", "count")):
            return random.choice([0, 1])
        elif column_name.lower() in ["patientid", "accessionnumber", "medrecno"]:
            return random.randint(0, self.int_max)
        else:
            return 0

    def generate_decimal_advanced(self, column_type):
        if column_type.lower() in ("decimal", 'numeric', "float", "double"):
            return random.randint(0, self.decimal_max)
        else:
            return 0.0

    def generate_time_advanced(self, column_type):
        mimesis_gen = mimesis.Generic(Locale.ZH)
        if column_type.lower() in ["datetime", "timestamp"]:
            return mimesis_gen.datetime.datetime(start=self.year_range[0], end=self.year_range[1]).strftime(
                "%Y-%m-%d %H:%M:%S")
        elif column_type.lower() == "date":
            return mimesis_gen.datetime.date(start=self.year_range[0], end=self.year_range[1]).strftime("%Y-%m-%d")
        elif column_type.lower() == "time":
            return mimesis_gen.datetime.time().strftime("%H:%M:%S")
        else:
            return ""

    # 数据生成器
    def data_generator_simple(self, column_type, column_length):
        mimesis_gen = mimesis.Generic(Locale.ZH)
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
            "date": lambda: mimesis_gen.datetime.date().strftime("%Y-%m-%d"),
            "datetime": lambda: mimesis_gen.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S"),
            "time": lambda: mimesis_gen.datetime.time().strftime("%H:%M:%S"),
            "timestamp": lambda: mimesis_gen.datetime.datetime().strftime("%Y-%m-%d %H:%M:%S")
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
            "timestamp": lambda: self.generate_time_advanced("timestamp")
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
    pass
