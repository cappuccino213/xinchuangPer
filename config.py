"""
@File : config.py 解析配置文件
@Date : 2024/6/27 下午3:02
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import toml
import os

# 为避免其他次级目录的访问，需要设置模块所在的完整路径
current_dir = os.path.dirname(os.path.abspath(__file__))

file_path = os.path.join(current_dir, "configuration.toml")

"""
这个方法会导致出现换行时，无法识别之后的配置项
def parse_config():
    with open(file_path, 'rb') as f:
        toml_data = f.read()
    data = toml.loads(toml_data.decode('utf-8'))
    return data
"""


def parse_config():
    with open(file_path, 'r', encoding='utf-8') as f:
        data = toml.load(f)
    return data


GLOBAL_CONFIG = parse_config()

if __name__ == "__main__":
    test_cases = GLOBAL_CONFIG["test_cases"]
    [print(i) for i in test_cases["query_per_test"]]
    # covert_before = test_cases["query_per_test"][2]["kingbase_query"]
    # from utils.text_tool import StringTool
    # covert_after = StringTool.remove_escape_chars(covert_before)
    # print(covert_before)
    # print(covert_after)
