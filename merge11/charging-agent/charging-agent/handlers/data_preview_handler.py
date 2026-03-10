# handlers/data_preview_handler.py - 数据预览处理（Pandas优化版）

from typing import Dict, Optional, List
import pandas as pd
import streamlit as st
from sqlalchemy import text
from utils.db_utils import create_db_engine, get_shared_engine
from data.error_handler import ErrorHandler


class DataPreviewHandler:
    """
    数据预览处理器（Pandas优化版）
    
    优先使用 PandasDataService 的内存缓存（毫秒级响应），
    如果缓存不可用，降级使用SQL查询。
    """
    
    # 核心预览字段（精简版：8个关键字段）
    CORE_PREVIEW_FIELDS = [
        '运营商名称', 
        '省份_中文',
        '城市_中文',
        '区县_中文',
        '充电站位置',
        '充电桩类型_转换',
        '额定功率',
        '充电站投入使用时间'
    ]
    
    # 最大预览行数限制
    MAX_PREVIEW_LIMIT = 1000
    
    def __init__(self, table_name: str):
        """
        初始化数据预览处理器
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self._engine = None
        self._pandas_service = None
    
    @property
    def engine(self):
        """获取数据库引擎（懒加载，使用共享连接池）"""
        if self._engine is None:
            self._engine = get_shared_engine()
        return self._engine
    
    @property
    def pandas_service(self):
        """获取Pandas数据服务（懒加载）"""
        if self._pandas_service is None:
            try:
                from services.pandas_data_service import PandasDataService
                self._pandas_service = PandasDataService.get_instance()
            except ImportError:
                self._pandas_service = None
        return self._pandas_service
    
    def preview_data(self, limit: int = 10, use_core_fields: bool = True, random_sample: bool = True) -> Dict:
        """
        预览数据（优化版：自动触发Pandas缓存加载）
        :param limit: 预览行数（默认10条，最大1000）
        :param use_core_fields: 是否只使用核心字段（默认True，大幅提升性能）
        :param random_sample: 是否随机抽取数据（默认True，避免总是显示相同的数据）
        :return: 预览结果字典
        """
        try:
            # 限制最大预览行数
            actual_limit = min(limit, self.MAX_PREVIEW_LIMIT)
            
            # 使用Pandas缓存（自动触发加载）
            if self.pandas_service:
                # 自动加载缓存（首次约30秒，后续毫秒级）
                return self._preview_from_pandas(actual_limit, use_core_fields)
            
            # 降级使用SQL查询（仅当pandas_service不可用时）
            return self._preview_from_sql(actual_limit, use_core_fields, random_sample)
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "数据预览")
            return {
                'success': False,
                'error': error_info['error_message'],
                'error_details': error_info
            }
    
    def _preview_from_pandas(self, limit: int, use_core_fields: bool) -> Dict:
        """从Pandas缓存预览数据（首次加载后毫秒级）"""
        # 自动触发缓存加载（首次约30秒）
        df = self.pandas_service.preview(limit, use_core_fields)
        
        return {
            'success': True,
            'data': df,
            'row_count': len(df),
            'column_count': len(df.columns),
            'is_core_fields': use_core_fields,
            'actual_limit': limit,
            'random_sample': True,
            'source': 'pandas_cache'
        }
    
    def _preview_from_sql(self, limit: int, use_core_fields: bool, random_sample: bool) -> Dict:
        """从SQL查询预览数据（降级方案）"""
        # 随机排序子句
        order_clause = "ORDER BY RAND()" if random_sample else ""
        
        # 过滤已删除数据（使用is_active高效过滤）
        where_clause = "WHERE is_active = 1"
        
        if use_core_fields:
            # 优化：只查询核心字段，减少数据传输
            fields_sql = ', '.join([f'`{f}`' for f in self.CORE_PREVIEW_FIELDS])
            query = text(f"SELECT {fields_sql} FROM `{self.table_name}` {where_clause} {order_clause} LIMIT {limit}")
        else:
            # 兼容模式：查询全部字段
            query = text(f"SELECT * FROM `{self.table_name}` {where_clause} {order_clause} LIMIT {limit}")
        
        df = pd.read_sql(query, self.engine)
        
        return {
            'success': True,
            'data': df,
            'row_count': len(df),
            'column_count': len(df.columns),
            'is_core_fields': use_core_fields,
            'actual_limit': limit,
            'random_sample': random_sample,
            'source': 'sql_query'
        }
    
    def preview_data_full(self, limit: int = 10) -> Dict:
        """
        预览完整数据（包含所有字段，性能较低）
        :param limit: 预览行数（默认10条，最大1000）
        :return: 预览结果字典
        """
        return self.preview_data(limit=limit, use_core_fields=False)
    
    def get_table_info(self) -> Dict:
        """
        获取表信息（自动使用缓存）
        :return: 表信息字典
        """
        try:
            # 使用Pandas缓存（自动触发加载）
            if self.pandas_service:
                # 触发加载（如果尚未加载）
                df = self.pandas_service.get_dataframe()
                return {
                    'success': True,
                    'total_rows': len(df),
                    'column_count': len(df.columns),
                    'columns': list(df.columns),
                    'source': 'pandas_cache'
                }
            
            # 降级使用SQL查询（仅当pandas_service不可用时）
            query = text(f"SELECT COUNT(*) as total FROM `{self.table_name}` WHERE is_active = 1")
            result = pd.read_sql(query, self.engine)
            total_rows = result.iloc[0]['total'] if not result.empty else 0
            
            # 获取列信息
            query_columns = text(f"SHOW COLUMNS FROM `{self.table_name}`")
            columns_df = pd.read_sql(query_columns, self.engine)
            
            return {
                'success': True,
                'total_rows': int(total_rows),
                'column_count': len(columns_df),
                'columns': columns_df['Field'].tolist(),
                'source': 'sql_query'
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "获取表信息")
            return {
                'success': False,
                'error': error_info['error_message'],
                'error_details': error_info
            }


