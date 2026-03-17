"""公共工具函数"""
import random
import string


def generate_random_string(length=5):
    """生成随机字母数字组合，用于文件名防重复"""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))
