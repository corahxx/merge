# handlers/data_compare_handler.py - 数据比对处理

from typing import Dict, Optional
from pathlib import Path
import streamlit as st

from data.data_processor import DataProcessor
from data.error_handler import ErrorHandler


class DataCompareHandler:
    """处理数据比对逻辑"""
    
    def __init__(self, table_name: str):
        """
        初始化数据比对处理器
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self.processor = DataProcessor(table_name=table_name, verbose=False)
    
    def compare_files(self,
                     file1_path: str,
                     file2_path: str,
                     key_column: str = '充电桩编号',
                     sheet1_name: Optional[str] = None,
                     sheet2_name: Optional[str] = None) -> Dict:
        """
        比对两个文件
        :param file1_path: 文件1路径
        :param file2_path: 文件2路径
        :param key_column: 比对键字段
        :param sheet1_name: 文件1工作表名
        :param sheet2_name: 文件2工作表名
        :return: 比对结果字典
        """
        try:
            result = self.processor.compare_files(
                file1_path,
                file2_path,
                key_column=key_column,
                sheet1_name=sheet1_name,
                sheet2_name=sheet2_name
            )
            
            if result.get('status') == 'success':
                return {
                    'success': True,
                    'file1_count': result.get('file1_count', 0),
                    'file2_count': result.get('file2_count', 0),
                    'difference_count': result.get('difference_count', 0),
                    'differences': result.get('differences'),
                    'common_records': result.get('common_records', 0),
                    'only_in_file1': result.get('only_in_df1', 0),
                    'only_in_file2': result.get('only_in_df2', 0)
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', '未知错误')
                }
                
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "数据比对")
            return {
                'success': False,
                'error': error_info['error_message'],
                'error_details': error_info
            }
    
    def get_compare_summary(self, result: Dict) -> Dict:
        """
        获取比对结果摘要
        :param result: 比对结果字典
        :return: 摘要信息字典
        """
        if result.get('success'):
            return {
                'file1_count': result.get('file1_count', 0),
                'file2_count': result.get('file2_count', 0),
                'difference_count': result.get('difference_count', 0),
                'common_records': result.get('common_records', 0),
                'only_in_file1': result.get('only_in_file1', 0),
                'only_in_file2': result.get('only_in_file2', 0),
                'has_differences': result.get('difference_count', 0) > 0
            }
        else:
            return {
                'error': result.get('error', '未知错误')
            }

