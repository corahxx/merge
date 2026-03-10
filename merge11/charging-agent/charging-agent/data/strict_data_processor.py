# -*- coding: utf-8 -*-
# data/strict_data_processor.py - 严格数据处理器
# 集成StrictDataCleaner，支持大文件分批处理
# 目标：数据准确率≥98%，批次成功率<95%暂停导入

import pandas as pd
from typing import Dict, Optional, Callable
from pathlib import Path
import gc

from .excel_reader_large import ExcelReaderLarge
from .strict_data_cleaner import StrictDataCleaner
from .data_loader import DataLoader
from .error_handler import ErrorHandler


class StrictDataProcessor:
    """
    严格数据处理器
    
    基于StrictDataCleaner实现，特点：
    1. 严格的数据验证规则
    2. 批次成功率<95%自动暂停
    3. 详细的错误日志记录
    4. 支持大文件分批处理
    """
    
    def __init__(self, table_name: str = 'evdata',
                 chunk_size: int = 500,
                 verbose: bool = True,
                 progress_callback: Optional[Callable] = None,
                 auto_pause_on_low_quality: bool = True):
        """
        初始化严格数据处理器
        
        :param table_name: 目标数据库表名
        :param chunk_size: 每批处理的行数（默认500，较小以便更精确控制）
        :param verbose: 是否显示详细日志
        :param progress_callback: 进度回调函数 callback(stage, current, total, message)
        :param auto_pause_on_low_quality: 成功率<95%时是否自动暂停
        """
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.verbose = verbose
        self.progress_callback = progress_callback
        self.auto_pause_on_low_quality = auto_pause_on_low_quality
        
        # 初始化数据加载器
        self.loader = DataLoader(table_name=table_name, verbose=False)
        
        # 清洗器会在process_file时初始化（需要源文件名）
        self.cleaner: Optional[StrictDataCleaner] = None
        
        # 处理结果
        self.process_result = {
            'status': 'pending',
            'total_rows_read': 0,
            'total_rows_success': 0,
            'total_rows_failed': 0,
            'chunks_processed': 0,
            'success_rate': 0.0,
            'paused': False,
            'pause_reason': '',
            'errors': [],
            'batch_id': '',
        }
    
    def _notify_progress(self, stage: str, current: int, total: int, message: str = ""):
        """通知进度"""
        if self.progress_callback:
            self.progress_callback(stage, current, total, message)
        elif self.verbose:
            if total > 0:
                percentage = current / total * 100
                print(f"📊 {stage}: {current}/{total} ({percentage:.1f}%) {message}")
            else:
                print(f"📊 {stage}: {current} {message}")
    
    def process_file(self, file_path: str,
                    field_mapping: Optional[Dict[str, str]] = None,
                    sheet_name: Optional[str] = None,
                    if_exists: str = 'append') -> Dict:
        """
        处理文件的完整流程：分块读取 -> 严格清洗 -> 入库
        
        :param file_path: 文件路径
        :param field_mapping: 字段映射字典
        :param sheet_name: 工作表名称（Excel）
        :param if_exists: 导入模式（append/replace）
        :return: 处理结果字典
        """
        file_name = Path(file_path).name
        
        # 重置结果
        self.process_result = {
            'status': 'pending',
            'file_path': file_path,
            'file_name': file_name,
            'total_rows_read': 0,
            'total_rows_success': 0,
            'total_rows_failed': 0,
            'total_rows_warning': 0,
            'chunks_processed': 0,
            'success_rate': 0.0,
            'paused': False,
            'pause_reason': '',
            'errors': [],
            'batch_id': '',
        }
        
        try:
            # 初始化严格清洗器
            self.cleaner = StrictDataCleaner(
                engine=self.loader.engine,
                source_file=file_name,
                verbose=self.verbose,
                batch_size=self.chunk_size
            )
            self.process_result['batch_id'] = self.cleaner.batch_id
            
            # 初始化读取器
            reader = ExcelReaderLarge(file_path, chunk_size=self.chunk_size)
            file_info = reader.get_file_info()
            
            if self.verbose:
                print(f"\n{'='*60}")
                print(f"📂 开始严格数据处理")
                print(f"{'='*60}")
                print(f"文件: {file_info['file_name']}")
                print(f"大小: {file_info['file_size_mb']:.2f} MB")
                print(f"批次大小: {self.chunk_size} 行")
                print(f"批次ID: {self.cleaner.batch_id}")
                print(f"{'='*60}\n")
            
            # 估算总行数
            estimated_total = None
            if file_info['file_ext'] == '.csv':
                estimated_total = reader._estimate_csv_rows()
            
            # 分块处理
            chunk_count = 0
            first_chunk = True
            current_row = 0
            
            for chunk_df in reader.read_chunks(sheet_name=sheet_name):
                chunk_count += 1
                chunk_size = len(chunk_df)
                
                # 更新进度
                if estimated_total and estimated_total > 0:
                    self._notify_progress("读取", current_row + chunk_size, estimated_total,
                                         f"第{chunk_count}批")
                else:
                    self._notify_progress("读取", current_row + chunk_size, -1,
                                         f"第{chunk_count}批 ({chunk_size}行)")
                
                if self.verbose:
                    print(f"\n📦 处理第 {chunk_count} 批数据 ({chunk_size} 行, 行号 {current_row+1}-{current_row+chunk_size})...")
                
                try:
                    # 字段映射
                    if field_mapping:
                        chunk_df = chunk_df.rename(columns={
                            k: v for k, v in field_mapping.items() if k in chunk_df.columns
                        })
                    
                    # 严格数据清洗
                    self._notify_progress("清洗", chunk_count, -1, f"严格校验第{chunk_count}批")
                    
                    success_df, failed_df, stats = self.cleaner.clean_batch(
                        chunk_df, start_row=current_row
                    )
                    
                    # 更新统计
                    self.process_result['total_rows_read'] += chunk_size
                    self.process_result['total_rows_success'] += len(success_df)
                    self.process_result['total_rows_failed'] += len(failed_df)
                    self.process_result['total_rows_warning'] = stats.get('warning_rows', 0)
                    
                    # 检查是否应该暂停
                    if self.auto_pause_on_low_quality:
                        should_pause, reason = self.cleaner.check_should_pause()
                        if should_pause:
                            self.process_result['paused'] = True
                            self.process_result['pause_reason'] = reason
                            self.process_result['status'] = 'paused'
                            
                            if self.verbose:
                                print(f"\n🛑 导入已暂停: {reason}")
                                print("请检查源数据质量后重新导入")
                            
                            # 保存已处理的数据和日志
                            self._save_results(success_df, if_exists if first_chunk else 'append')
                            self._finalize()
                            
                            return self.process_result
                    
                    # 入库成功的数据
                    if not success_df.empty:
                        self._notify_progress("入库", chunk_count, -1, 
                                            f"入库{len(success_df)}条")
                        
                        current_if_exists = if_exists if first_chunk else 'append'
                        load_result = self.loader.load(
                            success_df,
                            if_exists=current_if_exists,
                            chunk_size=min(self.chunk_size, 500)  # 入库批次更小
                        )
                        
                        if load_result.get('errors'):
                            for err in load_result['errors']:
                                self.process_result['errors'].append(f"批次{chunk_count}入库: {err}")
                        
                        first_chunk = False
                    
                    self.process_result['chunks_processed'] = chunk_count
                    
                    if self.verbose:
                        batch_rate = len(success_df) / chunk_size if chunk_size > 0 else 0
                        print(f"✅ 第{chunk_count}批完成: 成功{len(success_df)}, 失败{len(failed_df)}, 成功率{batch_rate:.1%}")
                    
                    # 更新行号
                    current_row += chunk_size
                    
                    # 释放内存
                    del chunk_df, success_df, failed_df
                    gc.collect()
                    
                except Exception as e:
                    error_info = ErrorHandler.handle_exception(e, f"处理第{chunk_count}批数据")
                    self.process_result['errors'].append(f"批次{chunk_count}: {error_info['error_message']}")
                    
                    if self.verbose:
                        print(f"❌ 第{chunk_count}批处理失败: {error_info['error_message']}")
                    
                    current_row += chunk_size
                    continue
            
            # 处理完成
            self._finalize()
            
            # 计算最终成功率
            total = self.process_result['total_rows_read']
            success = self.process_result['total_rows_success']
            self.process_result['success_rate'] = success / total if total > 0 else 0
            
            # 确定最终状态
            if self.process_result['success_rate'] >= 0.98:
                self.process_result['status'] = 'success'
            elif self.process_result['success_rate'] >= 0.95:
                self.process_result['status'] = 'partial_success'
            else:
                self.process_result['status'] = 'warning'
            
            if self.verbose:
                self.cleaner.print_summary()
            
            return self.process_result
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, f"处理文件: {file_path}")
            self.process_result['status'] = 'error'
            self.process_result['errors'].append(error_info['error_message'])
            
            if self.verbose:
                print(ErrorHandler.format_error_report(error_info))
            
            return self.process_result
    
    def _save_results(self, success_df: pd.DataFrame, if_exists: str):
        """保存处理结果"""
        if not success_df.empty:
            try:
                self.loader.load(success_df, if_exists=if_exists, chunk_size=500)
            except Exception as e:
                self.process_result['errors'].append(f"保存数据失败: {str(e)}")
    
    def _finalize(self):
        """完成处理，保存日志"""
        if self.cleaner:
            # 保存错误日志到数据库
            self.cleaner.save_error_logs_to_db()
            
            # 保存批次汇总
            self.cleaner.save_batch_summary_to_db()
    
    def get_result(self) -> Dict:
        """获取处理结果"""
        return self.process_result.copy()
    
    def get_error_summary(self) -> Dict:
        """获取错误汇总"""
        if not self.cleaner:
            return {'error_count': 0, 'errors': []}
        
        stats = self.cleaner.get_stats()
        
        # 按错误类型分组
        error_types = {}
        for log in self.cleaner.error_logs:
            error_type = log.get('error_type', 'UNKNOWN')
            if error_type not in error_types:
                error_types[error_type] = 0
            error_types[error_type] += 1
        
        return {
            'error_count': stats.get('error_count', 0),
            'error_types': error_types,
            'sample_errors': self.cleaner.error_logs[:20],  # 前20条示例
        }
