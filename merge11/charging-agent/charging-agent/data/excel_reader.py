# data/excel_reader.py - EXCEL文件读取器

import pandas as pd
import os
from typing import List, Dict, Optional, Tuple
from pathlib import Path


class ExcelReader:
    """
    EXCEL文件读取器
    支持读取.xlsx, .xls, .csv等格式
    """
    
    def __init__(self, file_path: str):
        """
        初始化EXCEL读取器
        :param file_path: EXCEL文件路径
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        self.file_ext = self.file_path.suffix.lower()
        self.df: Optional[pd.DataFrame] = None
        
    def read(self, sheet_name: Optional[str] = None, header: int = 0, 
             skip_rows: int = 0, n_rows: Optional[int] = None) -> pd.DataFrame:
        """
        读取EXCEL文件
        :param sheet_name: 工作表名称，None表示读取第一个工作表
        :param header: 表头所在行（0-based）
        :param skip_rows: 跳过的行数
        :param n_rows: 读取的行数，None表示读取全部
        :return: DataFrame对象
        """
        try:
            if self.file_ext == '.xlsx':
                # 读取xlsx文件
                if sheet_name:
                    self.df = pd.read_excel(
                        self.file_path, 
                        sheet_name=sheet_name,
                        header=header,
                        skiprows=skip_rows,
                        nrows=n_rows,
                        engine='openpyxl'
                    )
                else:
                    self.df = pd.read_excel(
                        self.file_path,
                        header=header,
                        skiprows=skip_rows,
                        nrows=n_rows,
                        engine='openpyxl'
                    )
            elif self.file_ext == '.xls':
                # 读取xls文件（旧版Excel格式）
                self.df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name,
                    header=header,
                    skiprows=skip_rows,
                    nrows=n_rows,
                    engine='xlrd'
                )
            elif self.file_ext == '.csv':
                # 读取CSV文件
                self.df = pd.read_csv(
                    self.file_path,
                    header=header,
                    skiprows=skip_rows,
                    nrows=n_rows,
                    encoding='utf-8-sig'  # 支持中文BOM
                )
            else:
                raise ValueError(f"不支持的文件格式: {self.file_ext}")
            
            return self.df
            
        except Exception as e:
            raise Exception(f"读取EXCEL文件失败: {str(e)}")
    
    def get_sheet_names(self) -> List[str]:
        """
        获取所有工作表名称（仅适用于Excel文件）
        :return: 工作表名称列表
        """
        if self.file_ext in ['.csv']:
            return ['Sheet1']
        
        try:
            if self.file_ext == '.xlsx':
                excel_file = pd.ExcelFile(self.file_path, engine='openpyxl')
            elif self.file_ext == '.xls':
                excel_file = pd.ExcelFile(self.file_path, engine='xlrd')
            else:
                return []
            
            return excel_file.sheet_names
        except Exception as e:
            raise Exception(f"获取工作表名称失败: {str(e)}")
    
    def get_info(self) -> Dict:
        """
        获取文件基本信息
        :return: 包含文件信息的字典
        """
        info = {
            'file_name': self.file_path.name,
            'file_path': str(self.file_path),
            'file_size': self.file_path.stat().st_size,
            'file_ext': self.file_ext,
            'sheet_names': self.get_sheet_names(),
        }
        
        if self.df is not None:
            info.update({
                'rows': len(self.df),
                'columns': len(self.df.columns),
                'column_names': list(self.df.columns),
                'memory_usage': self.df.memory_usage(deep=True).sum(),
            })
        
        return info
    
    def preview(self, n_rows: int = 5) -> pd.DataFrame:
        """
        预览数据（前n行）
        :param n_rows: 预览行数
        :return: DataFrame前n行
        """
        if self.df is None:
            raise ValueError("请先调用read()方法读取数据")
        return self.df.head(n_rows)
    
    def get_dataframe(self) -> pd.DataFrame:
        """
        获取DataFrame对象
        :return: DataFrame对象
        """
        if self.df is None:
            raise ValueError("请先调用read()方法读取数据")
        return self.df

