# data/data_processor.py - 数据处理主模块

import pandas as pd
from typing import Dict, Optional, List
from datetime import date
from pathlib import Path

from .excel_reader import ExcelReader
from .data_cleaner import DataCleaner
from .data_loader import DataLoader
from .data_analyzer import DataAnalyzer
from .error_handler import ErrorHandler


class DataProcessor:
    """
    数据处理主类
    整合EXCEL读取、数据清洗、数据入库、数据分析等功能
    """
    
    def __init__(self, table_name: str = 'table2509ev', verbose: bool = True):
        """
        初始化数据处理器
        :param table_name: 目标数据库表名
        :param verbose: 是否显示详细日志
        """
        self.table_name = table_name
        self.verbose = verbose
        
        # 初始化各个组件
        # 获取表结构用于类型转换
        temp_loader = DataLoader(table_name=table_name, verbose=False)
        table_schema = temp_loader.get_table_schema()
        engine = temp_loader.engine
        
        # 使用table_name而不是table_schema，让DataCleaner自动使用TableSchemaDict
        self.cleaner = DataCleaner(verbose=verbose, table_name=table_name, engine=engine, use_fixed_schema=True)
        self.loader = DataLoader(table_name=table_name, verbose=verbose)
        self.analyzer = DataAnalyzer(table_name=table_name, verbose=verbose)
        
        self.process_stats = {
            'files_processed': 0,
            'total_rows_loaded': 0,
            'total_errors': 0,
            'files': []
        }
    
    def process_excel_file(self, file_path: str, 
                          field_mapping: Optional[Dict[str, str]] = None,
                          sheet_name: Optional[str] = None,
                          if_exists: str = 'append',
                          use_upsert: bool = False,
                          unique_key: str = '充电桩编号',
                          convert_types: bool = True) -> Dict:
        """
        处理单个EXCEL文件的完整流程：读取 -> 清洗 -> 入库
        :param file_path: EXCEL文件路径
        :param field_mapping: 字段映射字典（EXCEL列名 -> 数据库字段名）
        :param sheet_name: 工作表名称（None表示读取第一个工作表）
        :param if_exists: 如果表已存在如何处理
        :param use_upsert: 是否使用更新/插入模式
        :param unique_key: 唯一键字段名（用于upsert）
        :return: 处理结果字典
        """
        file_result = {
            'file_path': file_path,
            'file_name': Path(file_path).name,
            'status': 'pending',
            'rows_read': 0,
            'rows_cleaned': 0,
            'rows_loaded': 0,
            'errors': []
        }
        
        try:
            # 1. 读取EXCEL文件
            if self.verbose:
                print(f"\n📂 开始处理文件: {Path(file_path).name}")
            
            reader = ExcelReader(file_path)
            df = reader.read(sheet_name=sheet_name)
            file_result['rows_read'] = len(df)
            
            if self.verbose:
                print(f"✅ 成功读取 {len(df)} 行数据")
            
            # 2. 数据清洗
            df_cleaned = self.cleaner.clean(df, field_mapping=field_mapping)
            file_result['rows_cleaned'] = len(df_cleaned)
            
            # 3. 数据入库
            if use_upsert:
                load_result = self.loader.upsert(df_cleaned, unique_key=unique_key)
            else:
                load_result = self.loader.load(df_cleaned, if_exists=if_exists)
            
            file_result['rows_loaded'] = load_result.get('rows_loaded', 0)
            file_result['status'] = 'success'
            
            # 更新统计
            self.process_stats['files_processed'] += 1
            self.process_stats['total_rows_loaded'] += file_result['rows_loaded']
            
            if self.verbose:
                print(f"✅ 文件处理完成: {file_path}")
            
        except Exception as e:
            # 使用错误处理器
            error_info = ErrorHandler.handle_exception(e, f"处理文件: {file_path}")
            error_msg = f"{error_info['error_type']}: {error_info['error_message']}"
            
            file_result['status'] = 'error'
            file_result['errors'].append(error_msg)
            file_result['error_details'] = error_info
            self.process_stats['total_errors'] += 1
            
            if self.verbose:
                print(ErrorHandler.format_error_report(error_info))
            
            # 不抛出异常，而是返回错误结果
            return file_result
        
        finally:
            self.process_stats['files'].append(file_result)
        
        return file_result
    
    def process_multiple_files(self, file_paths: List[str],
                              field_mapping: Optional[Dict[str, str]] = None,
                              **kwargs) -> Dict:
        """
        批量处理多个EXCEL文件
        :param file_paths: 文件路径列表
        :param field_mapping: 字段映射字典
        :param kwargs: 其他参数（传递给process_excel_file）
        :return: 处理结果统计
        """
        results = []
        
        for file_path in file_paths:
            try:
                result = self.process_excel_file(
                    file_path, 
                    field_mapping=field_mapping,
                    **kwargs
                )
                results.append(result)
            except Exception as e:
                if self.verbose:
                    print(f"❌ 跳过文件 {file_path}: {str(e)}")
                continue
        
        if self.verbose:
            self._print_batch_stats()
        
        return {
            'total_files': len(file_paths),
            'successful': len([r for r in results if r['status'] == 'success']),
            'failed': len([r for r in results if r['status'] == 'error']),
            'results': results,
            'stats': self.process_stats
        }
    
    def compare_files(self, file1_path: str, file2_path: str,
                     key_column: str = '充电桩编号',
                     sheet1_name: Optional[str] = None,
                     sheet2_name: Optional[str] = None) -> Dict:
        """
        比对两个EXCEL文件
        :param file1_path: 文件1路径
        :param file2_path: 文件2路径
        :param key_column: 比对键列
        :param sheet1_name: 文件1工作表名
        :param sheet2_name: 文件2工作表名
        :return: 比对结果
        """
        # 读取两个文件
        reader1 = ExcelReader(file1_path)
        df1 = reader1.read(sheet_name=sheet1_name)
        
        reader2 = ExcelReader(file2_path)
        df2 = reader2.read(sheet_name=sheet2_name)
        
        # 清洗数据
        df1_cleaned = self.cleaner.clean(df1)
        df2_cleaned = self.cleaner.clean(df2)
        
        # 比对
        return self.analyzer.compare_datasets(df1_cleaned, df2_cleaned, key_column)
    
    def get_database_statistics(self, group_by: Optional[str] = None, limit: Optional[int] = None,
                              charge_type_filter: Optional[List[str]] = None,
                      date_field: Optional[str] = None, start_date: Optional[date] = None,
                      end_date: Optional[date] = None, operator_filter: Optional[List[str]] = None,
                      location_filter: Optional[List[str]] = None, power_filter: Optional[List[str]] = None,
                      power_op: Optional[str] = None, power_value: Optional[float] = None,
                      power_value_min: Optional[float] = None, power_value_max: Optional[float] = None,
                      region_filter: Optional[Dict[str, str]] = None) -> Dict:
        """
        获取数据库统计信息
        :param group_by: 分组字段（如 '运营商名称', '区县_中文', '所属充电站编号' 等）
        :param limit: 统计的记录数限制（None或0表示不限制，查询全部数据；>0时限制查询条数）
        :param date_field: 日期字段名（'充电站投入使用时间' 或 '充电桩生产日期'）
        :param start_date: 起始日期
        :param end_date: 结束日期
        :param operator_filter: 运营商名称列表，用于筛选（None表示不筛选）
        :param location_filter: 区域名称列表，用于筛选（None表示不筛选，兼容旧接口）
        :param power_filter: 功率区间列表，用于筛选（None表示不筛选）
        :param power_op: 功率比较方式（'无'/'大于'/'小于'/'等于'/'大于等于'/'小于等于'/'介于'），单值用 power_value，介于用 power_value_min/power_value_max
        :param power_value: 功率数值 (kW)，单值比较时使用
        :param power_value_min: 功率最小值 (kW)，仅当 power_op 为「介于」时使用
        :param power_value_max: 功率最大值 (kW)，仅当 power_op 为「介于」时使用
        :param region_filter: 三级区域筛选字典，格式：{'province': 'XX省', 'city': 'XX市', 'district': 'XX区'}（None表示不筛选）
        :param charge_type_filter: 充电类型列表（'直流'或'交流'），用于筛选（None表示不筛选）
        :return: 统计结果字典
        """
        return self.analyzer.get_statistics(
            group_by=group_by,
            limit=limit,
            date_field=date_field,
            start_date=start_date,
            end_date=end_date,
            operator_filter=operator_filter,
            location_filter=location_filter,
            power_filter=power_filter,
            power_op=power_op,
            power_value=power_value,
            power_value_min=power_value_min,
            power_value_max=power_value_max,
            region_filter=region_filter,
            charge_type_filter=charge_type_filter
        )
    
    def get_operator_list(self) -> List[str]:
        """
        获取所有运营商名称列表
        :return: 运营商名称列表
        """
        return self.analyzer.get_operator_list()
    
    def get_data_quality_report(self, limit: Optional[int] = None) -> Dict:
        """
        获取数据质量报告
        :param limit: 统计的记录数限制（None或0表示不限制，查询全部数据；>0时限制查询条数）
        :return: 数据质量报告字典
        """
        return self.analyzer.get_data_quality_report(limit=limit)
    
    def find_duplicates_in_database(self, key_column: str = '充电桩编号') -> pd.DataFrame:
        """查找数据库中的重复记录"""
        return self.analyzer.find_duplicates(key_column)
    
    def _print_batch_stats(self):
        """打印批量处理统计"""
        print("\n" + "="*60)
        print("📊 批量处理统计")
        print("="*60)
        print(f"处理文件数: {self.process_stats['files_processed']}")
        print(f"总加载行数: {self.process_stats['total_rows_loaded']}")
        print(f"总错误数: {self.process_stats['total_errors']}")
        print("="*60 + "\n")

