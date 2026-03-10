# data/data_processor_large.py - 大文件数据处理主模块（优化版）

import pandas as pd
from typing import Dict, Optional, List, Callable
from pathlib import Path
import gc

from .excel_reader_large import ExcelReaderLarge
from .data_cleaner import DataCleaner
from .data_loader import DataLoader
from .error_handler import ErrorHandler


class DataProcessorLarge:
    """
    大文件数据处理主类（优化版）
    专门优化用于处理600M+的大文件
    """
    
    def __init__(self, table_name: str = 'table2509ev', 
                 chunk_size: int = 5000,
                 verbose: bool = True,
                 progress_callback: Optional[Callable] = None):
        """
        初始化大文件数据处理器
        :param table_name: 目标数据库表名
        :param chunk_size: 每批处理的行数
        :param verbose: 是否显示详细日志
        :param progress_callback: 进度回调函数 callback(stage, current, total, message)
        """
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.verbose = verbose
        self.progress_callback = progress_callback
        
        # 初始化各个组件
        # 使用table_name而不是table_schema，让DataCleaner自动使用TableSchemaDict
        loader = DataLoader(table_name=table_name, verbose=False)
        engine = loader.engine
        
        self.cleaner = DataCleaner(verbose=False, table_name=table_name, engine=engine, use_fixed_schema=True)  # 关闭详细日志以提高性能
        self.loader = DataLoader(table_name=table_name, verbose=False)
        
        self.process_stats = {
            'total_rows_read': 0,
            'total_rows_cleaned': 0,
            'total_rows_loaded': 0,
            'chunks_processed': 0,
            'errors': [],
            'cleaning_stats': {
                'rows_before': 0,
                'rows_after': 0,
                'duplicates_removed': 0,
                'null_rows_removed': 0,
                'invalid_rows_removed': 0,
                'normalized_fields': 0,
                'columns_type_converted': 0,
                'strings_truncated': 0,
                'truncation_details': {}
            }
        }
    
    def _notify_progress(self, stage: str, current: int, total: int, message: str = ""):
        """通知进度"""
        if self.progress_callback:
            self.progress_callback(stage, current, total, message)
        elif self.verbose:
            percentage = (current / total * 100) if total > 0 else 0
            print(f"📊 {stage}: {current}/{total} ({percentage:.1f}%) {message}")
    
    def process_large_file(self, file_path: str,
                          field_mapping: Optional[Dict[str, str]] = None,
                          sheet_name: Optional[str] = None,
                          if_exists: str = 'append',
                          use_upsert: bool = False,
                          unique_key: str = '充电桩编号') -> Dict:
        """
        处理大文件的完整流程：分块读取 -> 清洗 -> 入库
        :param file_path: EXCEL文件路径
        :param field_mapping: 字段映射字典
        :param sheet_name: 工作表名称
        :param if_exists: 导入模式
        :param use_upsert: 是否使用更新/插入模式
        :param unique_key: 唯一键字段名
        :return: 处理结果字典
        """
        file_result = {
            'file_path': file_path,
            'file_name': Path(file_path).name,
            'status': 'pending',
            'total_rows_read': 0,
            'total_rows_cleaned': 0,
            'total_rows_loaded': 0,
            'chunks_processed': 0,
            'errors': []
        }
        
        try:
            # 初始化读取器
            reader = ExcelReaderLarge(file_path, chunk_size=self.chunk_size)
            file_info = reader.get_file_info()
            
            if self.verbose:
                print(f"\n📂 开始处理大文件: {file_info['file_name']}")
                print(f"   文件大小: {file_info['file_size_mb']:.2f} MB")
                print(f"   每批处理: {self.chunk_size} 行")
            
            # 如果是Excel文件且很大，给出警告
            if file_info['file_ext'] in ['.xlsx', '.xls'] and file_info['file_size_mb'] > 200:
                if self.verbose:
                    print("⚠️  大Excel文件建议转换为CSV格式以提高处理速度")
            
            # 分块处理
            chunk_count = 0
            first_chunk = True
            total_rows_read = 0
            
            # 估算总行数（用于进度显示）
            estimated_total = None
            if file_info['file_ext'] == '.csv':
                estimated_total = reader._estimate_csv_rows()
            
            for chunk_df in reader.read_chunks(sheet_name=sheet_name):
                chunk_count += 1
                total_rows_read += len(chunk_df)
                
                # 更新读取进度
                if estimated_total and estimated_total > 0:
                    # 使用估算的总行数来显示进度
                    read_progress = min(total_rows_read / estimated_total, 1.0)
                    self._notify_progress("读取", int(total_rows_read), int(estimated_total), 
                                         f"已读取 {total_rows_read} 行（估算总数: {estimated_total}）")
                else:
                    self._notify_progress("读取", total_rows_read, -1, f"已读取 {total_rows_read} 行")
                
                if self.verbose:
                    print(f"\n📦 处理第 {chunk_count} 批数据 ({len(chunk_df)} 行)...")
                
                try:
                    # 数据清洗
                    self._notify_progress("清洗", chunk_count, -1, f"正在清洗第 {chunk_count} 批")
                    chunk_cleaned = self.cleaner.clean(chunk_df, field_mapping=field_mapping)
                    
                    # 收集清洗统计信息（累积所有批次的统计）
                    chunk_cleaning_stats = self.cleaner.get_stats()
                    if chunk_cleaning_stats:
                        self.process_stats['cleaning_stats']['rows_before'] += chunk_cleaning_stats.get('rows_before', 0)
                        self.process_stats['cleaning_stats']['rows_after'] += chunk_cleaning_stats.get('rows_after', 0)
                        self.process_stats['cleaning_stats']['duplicates_removed'] += chunk_cleaning_stats.get('duplicates_removed', 0)
                        self.process_stats['cleaning_stats']['null_rows_removed'] += chunk_cleaning_stats.get('null_rows_removed', 0)
                        self.process_stats['cleaning_stats']['invalid_rows_removed'] += chunk_cleaning_stats.get('invalid_rows_removed', 0)
                        self.process_stats['cleaning_stats']['normalized_fields'] = max(
                            self.process_stats['cleaning_stats']['normalized_fields'],
                            chunk_cleaning_stats.get('normalized_fields', 0)
                        )
                        self.process_stats['cleaning_stats']['columns_type_converted'] = max(
                            self.process_stats['cleaning_stats']['columns_type_converted'],
                            chunk_cleaning_stats.get('columns_type_converted', 0)
                        )
                        self.process_stats['cleaning_stats']['strings_truncated'] += chunk_cleaning_stats.get('strings_truncated', 0)
                        # 合并截断详情
                        if 'truncation_details' in chunk_cleaning_stats:
                            for col_name, details in chunk_cleaning_stats['truncation_details'].items():
                                if col_name not in self.process_stats['cleaning_stats']['truncation_details']:
                                    self.process_stats['cleaning_stats']['truncation_details'][col_name] = {
                                        'max_length': details.get('max_length', 0),
                                        'truncated_count': 0,
                                        'max_original_length': 0
                                    }
                                self.process_stats['cleaning_stats']['truncation_details'][col_name]['truncated_count'] += details.get('truncated_count', 0)
                                self.process_stats['cleaning_stats']['truncation_details'][col_name]['max_original_length'] = max(
                                    self.process_stats['cleaning_stats']['truncation_details'][col_name]['max_original_length'],
                                    details.get('max_original_length', 0)
                                )
                    
                    if chunk_cleaned.empty:
                        if self.verbose:
                            print(f"⚠️  第 {chunk_count} 批清洗后数据为空，跳过入库")
                        file_result['chunks_processed'] = chunk_count
                        self.process_stats['total_rows_read'] += len(chunk_df)
                        self.process_stats['chunks_processed'] = chunk_count
                        continue
                    
                    file_result['total_rows_cleaned'] += len(chunk_cleaned)
                    
                    # 数据入库
                    self._notify_progress("入库", chunk_count, -1, f"正在入库第 {chunk_count} 批（{len(chunk_cleaned)} 行）")
                    
                    # 对于第一块，使用指定的if_exists模式
                    current_if_exists = if_exists if first_chunk else 'append'
                    
                    if use_upsert and not first_chunk:
                        # Upsert模式仅在第一批之后使用
                        # 注意：upsert内部会调用load方法，load方法会自动调整chunk_size
                        load_result = self.loader.upsert(chunk_cleaned, unique_key=unique_key)
                    else:
                        # load方法会根据列数自动调整chunk_size，这里传入原始值即可
                        # load方法内部会确保参数不超过限制
                        load_result = self.loader.load(
                            chunk_cleaned, 
                            if_exists=current_if_exists,
                            chunk_size=self.chunk_size  # 使用用户设置的chunk_size，load方法会自动调整
                        )
                    
                    rows_loaded_this_chunk = load_result.get('rows_loaded', 0)
                    
                    # 检查是否有错误
                    if load_result.get('errors'):
                        error_summary = f"第 {chunk_count} 批入库有 {len(load_result['errors'])} 个错误"
                        file_result['errors'].append(error_summary)
                        if self.verbose:
                            print(f"⚠️  {error_summary}")
                            for err in load_result['errors'][:3]:  # 只显示前3个错误
                                print(f"   - {err}")
                    
                    file_result['total_rows_loaded'] += rows_loaded_this_chunk
                    file_result['chunks_processed'] = chunk_count
                    
                    # 更新统计
                    self.process_stats['total_rows_read'] += len(chunk_df)
                    self.process_stats['total_rows_cleaned'] += len(chunk_cleaned)
                    self.process_stats['total_rows_loaded'] += rows_loaded_this_chunk
                    self.process_stats['chunks_processed'] = chunk_count
                    
                    if self.verbose:
                        print(f"✅ 第 {chunk_count} 批处理完成 (入库: {rows_loaded_this_chunk} 行)")
                    
                    first_chunk = False
                    
                    # 释放内存
                    del chunk_df, chunk_cleaned
                    gc.collect()
                    
                except Exception as e:
                    error_info = ErrorHandler.handle_exception(
                        e, 
                        f"处理第 {chunk_count} 批数据"
                    )
                    error_msg = error_info['error_message']
                    file_result['errors'].append(f"批次 {chunk_count}: {error_msg}")
                    
                    if self.verbose:
                        print(f"❌ 第 {chunk_count} 批处理失败: {error_msg}")
                    
                    # 继续处理下一批
                    continue
            
            file_result['total_rows_read'] = self.process_stats['total_rows_read']
            file_result['total_rows_cleaned'] = self.process_stats['total_rows_cleaned']
            file_result['total_rows_loaded'] = self.process_stats['total_rows_loaded']
            file_result['cleaning_stats'] = self.process_stats['cleaning_stats']
            file_result['status'] = 'success' if not file_result['errors'] else 'partial_success'
            
            if self.verbose:
                self._print_summary(file_result)
            
            return file_result
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, f"处理文件: {file_path}")
            file_result['status'] = 'error'
            file_result['errors'].append(error_info['error_message'])
            
            if self.verbose:
                print(ErrorHandler.format_error_report(error_info))
            
            return file_result
    
    def _print_summary(self, result: Dict):
        """打印处理摘要"""
        print("\n" + "="*60)
        print("📊 处理完成摘要")
        print("="*60)
        print(f"文件: {result['file_name']}")
        print(f"状态: {result['status']}")
        print(f"读取行数: {result['total_rows_read']:,}")
        print(f"清洗后行数: {result['total_rows_cleaned']:,}")
        print(f"入库行数: {result['total_rows_loaded']:,}")
        print(f"处理批次数: {result['chunks_processed']}")
        if result['errors']:
            print(f"错误数: {len(result['errors'])}")
        print("="*60 + "\n")

