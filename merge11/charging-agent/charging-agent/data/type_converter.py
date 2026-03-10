# data/type_converter.py - 数据类型转换器

import pandas as pd
import numpy as np
import re
from typing import Dict, Optional, Any
from datetime import datetime, date
from sqlalchemy import inspect


class TypeConverter:
    """
    根据数据库字段类型转换DataFrame列的数据类型
    """
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.conversion_stats = {
            'columns_converted': 0,
            'conversion_errors': 0,
            'errors': []
        }
    
    def convert_to_database_types(self, df: pd.DataFrame, table_schema: Dict, 
                                   engine) -> pd.DataFrame:
        """
        根据数据库表结构转换DataFrame列的类型
        :param df: 原始DataFrame
        :param table_schema: 数据库表结构字典（从get_table_schema获取）
        :param engine: SQLAlchemy engine
        :return: 转换后的DataFrame
        """
        if not table_schema:
            if self.verbose:
                print("⚠️  未获取到表结构，跳过类型转换")
            return df
        
        df_converted = df.copy()
        self.conversion_stats['columns_converted'] = 0
        self.conversion_stats['conversion_errors'] = 0
        self.conversion_stats['errors'] = []
        
        for col_name, col_info in table_schema.items():
            # 只处理DataFrame中存在的列
            if col_name not in df_converted.columns:
                continue
            
            db_type_str = str(col_info['type']).upper()
            
            try:
                # 根据数据库类型进行转换
                converted = self._convert_column(
                    df_converted[col_name], 
                    db_type_str, 
                    col_name,
                    col_info.get('nullable', True)
                )
                
                if converted is not None:
                    df_converted[col_name] = converted
                    self.conversion_stats['columns_converted'] += 1
                    
                    if self.verbose:
                        print(f"✅ 字段 '{col_name}' 已转换为 {db_type_str}")
                        
            except Exception as e:
                error_msg = f"转换字段 '{col_name}' 失败: {str(e)}"
                self.conversion_stats['errors'].append(error_msg)
                self.conversion_stats['conversion_errors'] += 1
                
                if self.verbose:
                    print(f"⚠️  {error_msg}")
        
        return df_converted
    
    def _convert_column(self, series: pd.Series, db_type: str, col_name: str, 
                       nullable: bool = True) -> Optional[pd.Series]:
        """
        转换单个列的数据类型
        :param series: pandas Series
        :param db_type: 数据库类型字符串
        :param col_name: 列名
        :param nullable: 是否允许空值
        :return: 转换后的Series
        """
        # 如果是VARCHAR、CHAR、TEXT等字符串类型
        if any(t in db_type for t in ['VARCHAR', 'CHAR', 'TEXT', 'STRING']):
            # 转换为字符串，处理NaN和空值
            str_series = series.astype(str)
            # 将'nan', 'None', 'NaN'替换为空字符串（MySQL会将空字符串视为NULL如果字段允许NULL）
            str_series = str_series.replace('nan', '')
            str_series = str_series.replace('None', '')
            str_series = str_series.replace('NaN', '')
            return str_series
        
        # 如果是INT、INTEGER等整数类型
        elif any(t in db_type for t in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'MEDIUMINT']):
            # 尝试转换为整数，确保没有.0（取整处理）
            try:
                # 先转换为浮点数（可以处理字符串数字和带小数点的数字，如"2201001.0"）
                numeric_series = pd.to_numeric(series, errors='coerce')
                # 对浮点数进行取整处理（去除.0），NaN保持不变
                # 使用numpy的round然后转换为int，确保去掉.0
                int_values = []
                for val in numeric_series:
                    if pd.isna(val):
                        int_values.append(pd.NA)
                    else:
                        # 取整，去掉.0
                        int_values.append(int(np.round(val)))
                # 转换为可空整数类型
                int_series = pd.Series(int_values, index=series.index, dtype='Int64')
                return int_series
            except Exception as e:
                # 如果转换失败，使用备选方案
                numeric_series = pd.to_numeric(series, errors='coerce')
                int_values = [int(np.round(val)) if pd.notna(val) else pd.NA for val in numeric_series]
                return pd.Series(int_values, index=series.index, dtype='Int64')
        
        # 如果是DECIMAL、NUMERIC、FLOAT、DOUBLE等浮点数类型
        elif any(t in db_type for t in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL']):
            return pd.to_numeric(series, errors='coerce')
        
        # 如果是DATE类型
        elif 'DATE' in db_type and 'DATETIME' not in db_type and 'TIMESTAMP' not in db_type:
            # 统一转换为YYYY-MM-DD格式（只允许横线，不允许斜杠），无法转换的设为NULL
            try:
                # 先清理空字符串和空白值，将它们转换为None
                cleaned_series = series.copy()
                # 将空字符串、空白字符串、'nan'、'None'等转换为None
                cleaned_series = cleaned_series.replace(['', ' ', 'nan', 'None', 'NaN', 'null', 'NULL'], None)
                # 对于字符串类型，去除首尾空白后检查是否为空，并将斜杠替换为横线
                if cleaned_series.dtype == 'object':
                    def clean_date_string(x):
                        if x is None:
                            return None
                        if isinstance(x, str):
                            x_stripped = x.strip()
                            if x_stripped == '':
                                return None
                            # 将斜杠替换为横线（如 2024/9/12 -> 2024-9-12）
                            # 注意：这里只是替换，日期解析由pd.to_datetime处理
                            x_normalized = x_stripped.replace('/', '-')
                            return x_normalized
                        return x
                    cleaned_series = cleaned_series.apply(clean_date_string)
                
                # 尝试多种日期格式转换（现在所有日期字符串都使用横线分隔符）
                datetime_series = pd.to_datetime(cleaned_series, errors='coerce', format='mixed')
                # 将datetime转换为date对象（YYYY-MM-DD格式）
                # 无法转换的值已经是NaT（会被视为NULL）
                if hasattr(datetime_series.dt, 'date'):
                    date_series = datetime_series.dt.date
                else:
                    date_series = datetime_series
                
                # 确保NaT/None/空字符串转换为None（NULL），其他有效的date对象保持原样
                result = []
                for original_val, dt_val, date_val in zip(series, datetime_series, date_series):
                    # 如果原始值是空字符串、空白或None，直接设为None
                    if (isinstance(original_val, str) and original_val.strip() == '') or original_val is None:
                        result.append(None)
                    elif pd.isna(dt_val):
                        result.append(None)  # 无法转换的设为None（NULL）
                    else:
                        result.append(date_val)  # 有效的日期对象（YYYY-MM-DD格式）
                
                return pd.Series(result, index=series.index, dtype='object')
            except Exception as e:
                # 如果转换失败，所有值都设为None
                if self.verbose:
                    print(f"⚠️  日期转换失败，所有值设为NULL: {str(e)}")
                return pd.Series([None] * len(series), index=series.index, dtype='object')
        
        # 如果是DATETIME、TIMESTAMP类型
        elif any(t in db_type for t in ['DATETIME', 'TIMESTAMP']):
            # 先清理空字符串和空白值
            cleaned_series = series.replace(['', ' ', 'nan', 'None', 'NaN', 'null', 'NULL'], None)
            if cleaned_series.dtype == 'object':
                cleaned_series = cleaned_series.apply(lambda x: None if isinstance(x, str) and x.strip() == '' else x)
            datetime_series = pd.to_datetime(cleaned_series, errors='coerce')
            # 将NaT转换为None
            return datetime_series.where(pd.notna(datetime_series), None)
        
        # 如果是TIME类型
        elif 'TIME' in db_type:
            # 转换为时间类型
            return pd.to_datetime(series, errors='coerce')
        
        # 如果是BOOLEAN、BOOL、TINYINT(1)类型
        elif any(t in db_type for t in ['BOOLEAN', 'BOOL']) or 'TINYINT(1)' in db_type:
            # 尝试转换为布尔值
            if series.dtype == bool:
                return series
            # 尝试识别常见的布尔值表示
            bool_map = {
                'true': True, 'True': True, 'TRUE': True, '1': True, 1: True,
                'false': False, 'False': False, 'FALSE': False, '0': False, 0: False,
                '是': True, '否': False, 'Y': True, 'N': False, 'y': True, 'n': False
            }
            return series.map(bool_map).fillna(series).astype('boolean')
        
        # 其他类型，保持原样或转换为字符串
        else:
            return series
    
    def get_conversion_stats(self) -> Dict:
        """获取转换统计信息"""
        return self.conversion_stats.copy()

