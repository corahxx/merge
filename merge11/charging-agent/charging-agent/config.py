# config.py - 系统配置
# 支持环境变量覆盖，优先级：环境变量 > 配置文件

import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '3306')),  # MySQL端口，默认3306，可通过环境变量DB_PORT覆盖
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'caam'),
    'database': os.getenv('DB_NAME', 'evdata')
}

# AI大模型配置
LLM_CONFIG = {
    'enabled': False,  # 是否启用AI大模型功能（默认关闭）
    'model': 'qwen3:30b',  # 模型名称
}