# handlers/data_quality_handler.py - 数据质量报告处理

from typing import Dict, Optional
import pandas as pd
import streamlit as st
from sqlalchemy import text
from utils.db_utils import create_db_engine

from data.data_processor import DataProcessor
from data.error_handler import ErrorHandler


class DataQualityHandler:
    """处理数据质量报告逻辑"""
    
    def __init__(self, table_name: str):
        """
        初始化数据质量处理器
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self.processor = DataProcessor(table_name=table_name, verbose=False)
        self._engine = None
    
    @property
    def engine(self):
        """获取数据库引擎（懒加载）"""
        if self._engine is None:
            self._engine = create_db_engine(echo=False)  # 使用统一工具函数
        return self._engine
    
    def get_quality_report(self, limit: Optional[int] = None) -> Dict:
        """
        获取数据质量报告
        :param limit: 统计记录数限制
        :return: 质量报告字典
        """
        try:
            quality_report = self.processor.get_data_quality_report(limit=limit)
            
            if 'error' in quality_report:
                return {
                    'success': False,
                    'error': quality_report['error']
                }
            
            return {
                'success': True,
                'report': quality_report
            }
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "数据质量报告")
            return {
                'success': False,
                'error': error_info['error_message'],
                'error_details': error_info
            }
    
    def get_quality_statistics(self, quality_report: Dict) -> pd.DataFrame:
        """
        获取质量统计DataFrame
        :param quality_report: 质量报告字典
        :return: 质量统计DataFrame
        """
        quality_data = []
        column_stats = quality_report.get('column_stats', {})
        
        for col, col_info in column_stats.items():
            quality_data.append({
                '字段名': col,
                '空值数': col_info['null_count'],
                '空值率': f"{col_info['null_percentage']:.2f}%",
                '唯一值数': col_info['unique_count'],
                '数据类型': col_info['data_type']
            })
        
        return pd.DataFrame(quality_data)
    
    def find_duplicates(self, key_column: str = '充电桩编号', limit: Optional[int] = None) -> pd.DataFrame:
        """
        查找重复记录（排除已删除数据）
        :param key_column: 唯一键字段
        :param limit: 限制返回数量
        :return: 重复记录DataFrame
        """
        try:
            # 过滤已删除数据（使用is_active高效过滤）
            where_clause = "WHERE is_active = 1"
            
            if limit:
                query = text(
                    f"SELECT {key_column}, COUNT(*) as cnt "
                    f"FROM `{self.table_name}` "
                    f"{where_clause} "
                    f"GROUP BY {key_column} "
                    f"HAVING cnt > 1 "
                    f"LIMIT {limit}"
                )
            else:
                query = text(
                    f"SELECT {key_column}, COUNT(*) as cnt "
                    f"FROM `{self.table_name}` "
                    f"{where_clause} "
                    f"GROUP BY {key_column} "
                    f"HAVING cnt > 1"
                )
            
            duplicates = pd.read_sql(query, self.engine)
            return duplicates
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "查找重复记录")
            st.error(f"查找重复记录失败: {error_info['error_message']}")
            return pd.DataFrame()

