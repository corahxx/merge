# utils/db_utils.py - 数据库工具函数

"""
数据库连接工具模块
统一管理数据库连接字符串构建和引擎创建逻辑
支持环境变量覆盖配置

优化特性：
- 连接池配置（pool_size=5, max_overflow=10）
- 连接健康检查（pool_pre_ping=True）
- 全局引擎单例（避免重复创建）
"""

from sqlalchemy import create_engine
from config import DB_CONFIG
import os

# 全局引擎单例（避免重复创建连接池）
_global_engine = None


def get_db_url() -> str:
    """
    获取数据库连接URL
    支持环境变量覆盖配置，优先级：环境变量 > config.py配置 > 默认值
    
    :return: 数据库连接URL字符串
    """
    # 优先使用环境变量，其次使用config.py配置，最后使用默认值
    user = os.getenv('DB_USER') or DB_CONFIG.get('user', 'root')
    password = os.getenv('DB_PASSWORD') or DB_CONFIG.get('password', '')
    host = os.getenv('DB_HOST') or DB_CONFIG.get('host', 'localhost')
    port = int(os.getenv('DB_PORT') or str(DB_CONFIG.get('port', 3306)))
    database = os.getenv('DB_NAME') or DB_CONFIG.get('database', 'evdata')
    
    # 构建连接URL（格式：mysql+pymysql://user:password@host:port/database）
    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    
    return db_url


def create_db_engine(echo: bool = False, **kwargs):
    """
    创建数据库引擎
    统一管理数据库引擎创建逻辑，内置连接池优化配置
    
    :param echo: 是否打印SQL语句（用于调试）
    :param kwargs: 其他create_engine参数，可覆盖默认连接池配置
    :return: SQLAlchemy引擎对象
    """
    db_url = get_db_url()
    
    # 默认连接池优化配置
    default_pool_config = {
        'pool_size': 5,           # 连接池保持的连接数
        'max_overflow': 10,       # 允许的最大溢出连接数
        'pool_pre_ping': True,    # 每次使用前检查连接是否有效
        'pool_recycle': 3600,     # 1小时后回收连接，避免MySQL断开
        'pool_timeout': 30,       # 获取连接的超时时间（秒）
    }
    
    # 用户传入的参数覆盖默认配置
    default_pool_config.update(kwargs)
    
    return create_engine(db_url, echo=echo, **default_pool_config)


def get_shared_engine():
    """
    获取全局共享的数据库引擎（单例模式）
    避免在应用中重复创建连接池，提高性能
    
    :return: SQLAlchemy引擎对象
    """
    global _global_engine
    if _global_engine is None:
        _global_engine = create_db_engine()
    return _global_engine


def dispose_shared_engine():
    """
    销毁全局共享引擎，释放所有连接
    通常在应用关闭时调用
    """
    global _global_engine
    if _global_engine is not None:
        _global_engine.dispose()
        _global_engine = None


def test_connection():
    """
    测试数据库连接
    
    :return: (success: bool, message: str) 元组
        - success: 连接是否成功
        - message: 连接结果消息
    """
    engine = None
    try:
        from sqlalchemy import text
        # 创建引擎，限制连接池大小，避免连接过多
        engine = create_db_engine(
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,  # 连接前检查连接是否有效
            pool_recycle=3600    # 1小时后回收连接
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "数据库连接成功"
    except Exception as e:
        return False, f"数据库连接失败: {str(e)}"
    finally:
        # 确保引擎和连接池被正确关闭
        if engine:
            try:
                engine.dispose()
            except:
                pass


def get_db_config_info() -> dict:
    """
    获取当前数据库配置信息（不包含密码）
    用于显示和调试
    
    :return: 配置信息字典
    """
    user = os.getenv('DB_USER') or DB_CONFIG.get('user', 'root')
    host = os.getenv('DB_HOST') or DB_CONFIG.get('host', 'localhost')
    port = int(os.getenv('DB_PORT') or str(DB_CONFIG.get('port', 3306)))
    database = os.getenv('DB_NAME') or DB_CONFIG.get('database', 'evdata')
    
    return {
        'host': host,
        'port': port,
        'user': user,
        'database': database,
        'password_set': bool(os.getenv('DB_PASSWORD') or DB_CONFIG.get('password'))
    }
