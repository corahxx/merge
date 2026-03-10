# -*- coding: utf-8 -*-
# data/table_schema.py - 数据库表结构字典

from typing import Dict, Optional
from sqlalchemy import inspect
from utils.db_utils import create_db_engine

# evdata表的固化字段结构（从数据库查询得出）
EVDATA_SCHEMA = {
    '序号': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '充电桩编号': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩内部编号': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '省份': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '城市': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '区县': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '充电桩类型': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩所属区域分类': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '所属充电站编号': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电站内部编号': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '充电站名称': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电站位置': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电站投入使用时间': {'type': 'DATE', 'nullable': True, 'default': None},
    '充电站所处道路属性': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电站联系电话': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '充电桩所属运营商': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '电表号': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩厂商编号': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩型号': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩属性': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩生产日期': {'type': 'DATE', 'nullable': True, 'default': None},
    '服务时间': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '桩型号是否获得联盟标识授权': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '支付方式': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '设备开通时间': {'type': 'DATE', 'nullable': True, 'default': None},
    '额定电压上限': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '额定电压下限': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '额定电流上限': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '额定电流下限': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '额定功率': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '接口数量': {'type': 'INTEGER', 'nullable': True, 'default': None},
    '接口1标准': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '接口2标准': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '接口3标准': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '接口4标准': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '备注': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '省份_中文': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '城市_中文': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '区县_中文': {'type': 'TEXT', 'nullable': True, 'default': None},
    '充电桩类型_转换': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩属性_转换': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩所属运营商_转换': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩厂商编号_转换': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '入库时间': {'type': 'DATE', 'nullable': True, 'default': None},
    '运营商名称': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    '充电桩内部编号_运营商名称': {'type': 'VARCHAR(255)', 'nullable': True, 'default': None},
    'UID': {'type': 'VARCHAR(255)', 'nullable': False, 'default': None},  # 唯一标识ID，使用UUID字符串
    '充电站所处道路性': {'type': 'VARCHAR(50)', 'nullable': True, 'default': None},
}

# 表名到固化结构的映射
FIXED_SCHEMAS = {
    'evdata': EVDATA_SCHEMA,
}


class TableSchemaDict:
    """
    数据库表结构字典类
    支持从数据库动态加载或使用固化数据
    """
    
    def __init__(self, table_name: str, use_fixed: bool = True):
        """
        初始化表结构字典
        :param table_name: 表名
        :param use_fixed: 是否优先使用固化的结构数据（如果可用）
        """
        self.table_name = table_name
        self.use_fixed = use_fixed
        self._schema: Optional[Dict] = None
        self._engine = None
    
    def get_schema(self, engine=None) -> Dict:
        """
        获取表结构
        :param engine: SQLAlchemy引擎（如果不提供，会创建新连接）
        :return: 字段结构字典 {字段名: {type, nullable, default}}
        """
        # 如果已有缓存，直接返回
        if self._schema is not None:
            return self._schema
        
        # 优先使用固化数据
        if self.use_fixed and self.table_name in FIXED_SCHEMAS:
            self._schema = FIXED_SCHEMAS[self.table_name].copy()
            return self._schema
        
        # 从数据库动态加载
        if engine is None:
            engine = create_db_engine(echo=False)
        
        try:
            inspector = inspect(engine)
            if self.table_name not in inspector.get_table_names():
                return {}
            
            columns = inspector.get_columns(self.table_name)
            schema = {}
            for col in columns:
                schema[col['name']] = {
                    'type': str(col['type']),
                    'nullable': col['nullable'],
                    'default': col.get('default')
                }
            
            self._schema = schema
            return schema
            
        except Exception as e:
            print(f"⚠️  获取表 {self.table_name} 结构失败: {str(e)}")
            # 如果数据库查询失败，尝试使用固化数据作为后备
            if self.table_name in FIXED_SCHEMAS:
                print(f"   使用固化的表结构数据")
                self._schema = FIXED_SCHEMAS[self.table_name].copy()
                return self._schema
            return {}
    
    def get_column_type(self, column_name: str, engine=None) -> Optional[str]:
        """
        获取指定字段的类型
        :param column_name: 字段名
        :param engine: SQLAlchemy引擎
        :return: 字段类型字符串
        """
        schema = self.get_schema(engine)
        if column_name in schema:
            return schema[column_name]['type']
        return None
    
    def get_column_info(self, column_name: str, engine=None) -> Optional[Dict]:
        """
        获取指定字段的完整信息
        :param column_name: 字段名
        :param engine: SQLAlchemy引擎
        :return: 字段信息字典 {type, nullable, default}
        """
        schema = self.get_schema(engine)
        return schema.get(column_name)
    
    def has_column(self, column_name: str, engine=None) -> bool:
        """
        检查表中是否有指定字段
        :param column_name: 字段名
        :param engine: SQLAlchemy引擎
        :return: 是否存在
        """
        schema = self.get_schema(engine)
        return column_name in schema
    
    def get_all_columns(self, engine=None) -> list:
        """
        获取所有字段名列表
        :param engine: SQLAlchemy引擎
        :return: 字段名列表
        """
        schema = self.get_schema(engine)
        return list(schema.keys())
    
    def clear_cache(self):
        """清除缓存，强制重新加载"""
        self._schema = None


# 便捷函数
def get_table_schema(table_name: str, use_fixed: bool = True, engine=None) -> Dict:
    """
    获取表结构的便捷函数
    :param table_name: 表名
    :param use_fixed: 是否优先使用固化数据
    :param engine: SQLAlchemy引擎
    :return: 字段结构字典
    """
    schema_dict = TableSchemaDict(table_name, use_fixed)
    return schema_dict.get_schema(engine)


def get_column_type(table_name: str, column_name: str, use_fixed: bool = True, engine=None) -> Optional[str]:
    """
    获取字段类型的便捷函数
    :param table_name: 表名
    :param column_name: 字段名
    :param use_fixed: 是否优先使用固化数据
    :param engine: SQLAlchemy引擎
    :return: 字段类型字符串
    """
    schema_dict = TableSchemaDict(table_name, use_fixed)
    return schema_dict.get_column_type(column_name, engine)


def extract_varchar_length(db_type: str) -> Optional[int]:
    """
    从数据库类型字符串中提取VARCHAR的长度
    :param db_type: 数据库类型字符串，如 'VARCHAR(255)', 'VARCHAR(50)' 等
    :return: VARCHAR的长度，如果不是VARCHAR类型则返回None
    """
    import re
    db_type_upper = str(db_type).upper()
    
    # 匹配VARCHAR(数字)或CHAR(数字)
    match = re.search(r'VARCHAR\((\d+)\)|CHAR\((\d+)\)', db_type_upper)
    if match:
        # 返回第一个非None的匹配组
        length = match.group(1) or match.group(2)
        return int(length)
    
    return None


def get_varchar_lengths(table_schema: Dict) -> Dict[str, int]:
    """
    从表结构中提取所有VARCHAR字段的长度信息
    :param table_schema: 表结构字典
    :return: 字段名到长度的映射字典，例如 {'字段名': 255, ...}
    """
    varchar_lengths = {}
    for col_name, col_info in table_schema.items():
        db_type = str(col_info.get('type', ''))
        length = extract_varchar_length(db_type)
        if length is not None:
            varchar_lengths[col_name] = length
    
    return varchar_lengths

