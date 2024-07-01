"""
@File : config.py
@Date : 2024/6/27 下午3:02
@Author: 九层风（YePing Zhang）
@Contact : yeahcheung213@163.com
"""
import toml
import os


# 为避免其他次级目录的访问，需要设置模块所在的完整路径
current_dir = os.path.dirname(os.path.abspath(__file__))

file_path = os.path.join(current_dir, "configuration.toml")



def parse_config():
    with open(file_path, 'rb') as f:
        toml_data = f.read()
    data = toml.loads(toml_data.decode('utf-8'))
    return data


CONFIG = parse_config()

if __name__ == "__main__":
    print(CONFIG)
