# auth/password_utils.py - 密码工具

import bcrypt
import re
from typing import Tuple


def hash_password(password: str) -> str:
    """
    对密码进行bcrypt哈希
    :param password: 明文密码
    :return: 哈希后的密码
    """
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    """
    验证密码是否正确
    :param password: 明文密码
    :param password_hash: 存储的哈希值
    :return: 是否匹配
    """
    try:
        return bcrypt.checkpw(
            password.encode('utf-8'),
            password_hash.encode('utf-8')
        )
    except Exception:
        return False


def validate_password_strength(password: str) -> Tuple[bool, str]:
    """
    验证密码强度
    要求：最少8位，包含字母和数字
    :param password: 密码
    :return: (是否合格, 提示信息)
    """
    if len(password) < 8:
        return False, "密码长度至少8位"
    
    if not re.search(r'[A-Za-z]', password):
        return False, "密码必须包含字母"
    
    if not re.search(r'\d', password):
        return False, "密码必须包含数字"
    
    return True, "密码强度合格"


def generate_temp_password() -> str:
    """
    生成临时密码（用于重置密码）
    :return: 8位临时密码
    """
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(8))
