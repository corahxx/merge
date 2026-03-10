# handlers/data_import_handler.py - 数据导入处理

from typing import Dict, Optional, Callable, List
from pathlib import Path
import streamlit as st

from data.data_processor import DataProcessor
from data.data_processor_large import DataProcessorLarge
from data.strict_data_processor import StrictDataProcessor
from data.error_handler import ErrorHandler


class DataImportHandler:
    """处理数据导入逻辑"""
    
    def __init__(self, table_name: str):
        """
        初始化数据导入处理器
        :param table_name: 目标数据库表名
        """
        self.table_name = table_name
        self.processor = DataProcessor(table_name=table_name, verbose=False)
        self.strict_processor = None  # 延迟初始化
    
    def import_file(self, 
                   file_path: str,
                   sheet_name: Optional[str] = None,
                   if_exists: str = 'append',
                   use_upsert: bool = False,
                   unique_key: str = '充电桩编号',
                   is_large_file: bool = False,
                   chunk_size: int = 5000,
                   progress_callback: Optional[Callable] = None,
                   use_strict_mode: bool = False) -> Dict:
        """
        导入文件数据
        :param file_path: 文件路径
        :param sheet_name: 工作表名称（Excel）
        :param if_exists: 导入模式（append/replace）
        :param use_upsert: 是否使用更新/插入模式
        :param unique_key: 唯一键字段名
        :param is_large_file: 是否为大文件
        :param chunk_size: 批次大小（每批处理的行数，仅大文件有效）
        :param progress_callback: 进度回调函数
        :param use_strict_mode: 是否使用严格模式（98%准确率目标，<95%暂停）
        :return: 处理结果字典
        """
        try:
            if use_strict_mode:
                # 严格模式：使用StrictDataProcessor
                self.strict_processor = StrictDataProcessor(
                    table_name=self.table_name,
                    chunk_size=min(chunk_size, 500),  # 严格模式使用更小的批次
                    verbose=True,
                    progress_callback=progress_callback,
                    auto_pause_on_low_quality=True
                )
                
                result = self.strict_processor.process_file(
                    file_path,
                    sheet_name=sheet_name,
                    if_exists=if_exists
                )
                
                # 转换结果格式以兼容现有代码
                return {
                    'status': result.get('status', 'error'),
                    'rows_read': result.get('total_rows_read', 0),
                    'rows_loaded': result.get('total_rows_success', 0),
                    'total_rows_read': result.get('total_rows_read', 0),
                    'total_rows_loaded': result.get('total_rows_success', 0),
                    'success_rate': result.get('success_rate', 0),
                    'failed_rows': result.get('total_rows_failed', 0),
                    'warning_rows': result.get('total_rows_warning', 0),
                    'batch_id': result.get('batch_id', ''),
                    'paused': result.get('paused', False),
                    'pause_reason': result.get('pause_reason', ''),
                    'errors': result.get('errors', []),
                    'cleaning_stats': {
                        'strict_mode': True,
                        'success_rate': result.get('success_rate', 0),
                    }
                }
            
            elif is_large_file:
                # 大文件处理（原有逻辑）
                processor_large = DataProcessorLarge(
                    table_name=self.table_name,
                    chunk_size=chunk_size,
                    verbose=True,
                    progress_callback=progress_callback
                )
                
                result = processor_large.process_large_file(
                    file_path,
                    sheet_name=sheet_name,
                    if_exists=if_exists,
                    use_upsert=use_upsert,
                    unique_key=unique_key
                )
            else:
                # 普通文件处理
                result = self.processor.process_excel_file(
                    file_path,
                    sheet_name=sheet_name,
                    if_exists=if_exists,
                    use_upsert=use_upsert,
                    unique_key=unique_key
                )
            
            return result
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "数据导入")
            # 确保返回的结果格式正确，包含所有必需的字段
            return {
                'status': 'error',
                'error': error_info.get('error_message', str(e)),
                'error_details': error_info,
                'rows_read': 0,
                'rows_loaded': 0,
                'total_rows_read': 0,
                'total_rows_loaded': 0
            }
    
    def get_strict_error_summary(self) -> Dict:
        """
        获取严格模式的错误汇总
        :return: 错误汇总字典
        """
        if self.strict_processor:
            return self.strict_processor.get_error_summary()
        return {'error_count': 0, 'errors': []}
    
    def get_import_result_summary(self, result: Dict) -> Dict:
        """
        获取导入结果摘要
        :param result: 导入结果字典
        :return: 摘要信息字典
        """
        if result.get('status') in ['success', 'partial_success']:
            return {
                'success': True,
                'rows_read': result.get('rows_read', result.get('total_rows_read', 0)),
                'rows_loaded': result.get('rows_loaded', result.get('total_rows_loaded', 0)),
                'status': result.get('status', 'success'),
                'cleaning_stats': result.get('cleaning_stats', {})
            }
        else:
            return {
                'success': False,
                'error': result.get('error', '未知错误'),
                'error_details': result.get('error_details', {})
            }
    
    def import_files_batch(self,
                          file_paths: List[Path],
                          if_exists: str = 'append',
                          use_upsert: bool = False,
                          unique_key: str = '充电桩编号',
                          chunk_size: int = 5000,
                          use_strict_mode: bool = False,
                          large_file_threshold_mb: int = 50,
                          progress_callback: Optional[Callable] = None) -> Dict:
        """
        批量导入多个文件
        
        :param file_paths: 文件路径列表
        :param if_exists: 导入模式（第一个文件使用此模式，后续文件强制 append）
        :param use_upsert: 是否使用更新/插入模式
        :param unique_key: 唯一键字段名
        :param chunk_size: 批次大小
        :param use_strict_mode: 是否使用严格模式
        :param large_file_threshold_mb: 大文件阈值（MB）
        :param progress_callback: 进度回调 callback(file_index, total_files, file_name, status)
        :return: 批量导入结果字典
        """
        results = {
            'total_files': len(file_paths),
            'success_files': 0,
            'failed_files': 0,
            'total_rows_read': 0,
            'total_rows_loaded': 0,
            'file_results': []
        }
        
        for i, file_path in enumerate(file_paths):
            file_path = Path(file_path)
            file_name = file_path.name
            
            # 通知进度
            if progress_callback:
                progress_callback(i, len(file_paths), file_name, 'processing')
            
            # 第一个文件使用指定模式，后续文件强制 append
            current_if_exists = if_exists if i == 0 else 'append'
            
            # 判断是否为大文件
            try:
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                is_large_file = file_size_mb > large_file_threshold_mb
            except:
                is_large_file = False
                file_size_mb = 0
            
            # 导入单个文件
            try:
                result = self.import_file(
                    file_path=str(file_path),
                    if_exists=current_if_exists,
                    use_upsert=use_upsert,
                    unique_key=unique_key,
                    is_large_file=is_large_file,
                    chunk_size=chunk_size,
                    use_strict_mode=use_strict_mode
                )
                
                file_result = {
                    'file': file_name,
                    'size_mb': round(file_size_mb, 2),
                    'status': result.get('status', 'unknown'),
                    'rows_read': result.get('rows_read', result.get('total_rows_read', 0)),
                    'rows_loaded': result.get('rows_loaded', result.get('total_rows_loaded', 0)),
                    'error': result.get('error', None)
                }
                
                if result.get('status') in ['success', 'partial_success']:
                    results['success_files'] += 1
                    if progress_callback:
                        progress_callback(i, len(file_paths), file_name, 'success')
                else:
                    results['failed_files'] += 1
                    if progress_callback:
                        progress_callback(i, len(file_paths), file_name, 'failed')
                
                results['total_rows_read'] += file_result['rows_read']
                results['total_rows_loaded'] += file_result['rows_loaded']
                
            except Exception as e:
                file_result = {
                    'file': file_name,
                    'size_mb': round(file_size_mb, 2),
                    'status': 'error',
                    'rows_read': 0,
                    'rows_loaded': 0,
                    'error': str(e)
                }
                results['failed_files'] += 1
                if progress_callback:
                    progress_callback(i, len(file_paths), file_name, 'failed')
            
            results['file_results'].append(file_result)
        
        # 设置总体状态
        if results['failed_files'] == 0:
            results['status'] = 'success'
        elif results['success_files'] == 0:
            results['status'] = 'failed'
        else:
            results['status'] = 'partial_success'
        
        return results


