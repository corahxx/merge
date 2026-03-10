# data/excel_reader_large.py - 大文件EXCEL读取器（优化版）

import pandas as pd
import os
from typing import List, Dict, Optional, Iterator, Callable
from pathlib import Path
import gc


class ExcelReaderLarge:
    """
    大文件EXCEL读取器（优化版）
    支持分块读取，节省内存
    适用于600M+的大文件
    """
    
    def __init__(self, file_path: str, chunk_size: int = 10000):
        """
        初始化大文件读取器
        :param file_path: EXCEL文件路径
        :param chunk_size: 每次读取的行数（CSV文件）
        """
        self.file_path = Path(file_path)
        if not self.file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        self.file_ext = self.file_path.suffix.lower()
        self.chunk_size = chunk_size
        self.file_size = self.file_path.stat().st_size
        
    def read_chunks(self, sheet_name: Optional[str] = None, 
                   header: int = 0,
                   callback: Optional[Callable] = None) -> Iterator[pd.DataFrame]:
        """
        分块读取文件（生成器）
        :param sheet_name: 工作表名称
        :param header: 表头所在行
        :param callback: 进度回调函数（已废弃，进度由上层处理器管理）
        :return: DataFrame迭代器
        """
        if self.file_ext == '.csv':
            # CSV文件可以流式读取
            yield from self._read_csv_chunks(header, callback)
        elif self.file_ext in ['.xlsx', '.xls']:
            # Excel文件需要一次性读取，但可以分批处理
            yield from self._read_excel_chunks(sheet_name, header, callback)
        else:
            raise ValueError(f"不支持的文件格式: {self.file_ext}")
    
    def _read_csv_chunks(self, header: int, callback: Optional[Callable]) -> Iterator[pd.DataFrame]:
        """分块读取CSV文件"""
        try:
            # 估算总行数（用于进度显示）
            total_estimated = self._estimate_csv_rows()
            
            current_rows = 0
            for chunk_df in pd.read_csv(
                self.file_path,
                header=header,
                chunksize=self.chunk_size,
                encoding='utf-8-sig',
                low_memory=False  # 禁用内存优化以加快速度
            ):
                current_rows += len(chunk_df)
                
                # 不在这里调用callback，让上层处理器来调用
                # callback调用已经移到了data_processor_large.py中
                
                yield chunk_df
                
                # 强制垃圾回收
                del chunk_df
                gc.collect()
                
        except Exception as e:
            raise Exception(f"分块读取CSV文件失败: {str(e)}")
    
    def _read_excel_chunks(self, sheet_name: Optional[str], 
                          header: int,
                          callback: Optional[Callable]) -> Iterator[pd.DataFrame]:
        """
        Excel文件分块处理
        注意：Excel文件无法真正流式读取，需要先读取到内存
        这里采用先读取整个文件，然后分批返回的策略
        """
        # 先验证文件完整性
        if not self._verify_file_integrity():
            raise Exception(
                f"Excel文件可能损坏或不完整。"
                f"文件大小: {self.file_size / 1024 / 1024:.2f}MB。"
                "建议：1) 检查文件是否完整下载；2) 尝试重新保存文件；3) 转换为CSV格式"
            )
        
        try:
            engine = 'openpyxl' if self.file_ext == '.xlsx' else 'xlrd'
            
            # 先读取整个文件（对于Excel，这是必需的）
            print("⏳ 正在读取Excel文件（大文件可能需要几分钟）...")
            
            # 尝试使用read_only模式以提高性能和内存效率
            try:
                df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name,
                    header=header,
                    engine=engine,
                    # openpyxl支持read_only模式，但pandas不直接支持，所以这里先尝试普通读取
                )
            except EOFError as e:
                raise Exception(
                    f"Excel文件读取中断（EOFError）。可能原因：\n"
                    f"1. 文件损坏或不完整\n"
                    f"2. 文件正在被其他程序使用\n"
                    f"3. 文件没有完全下载/复制\n"
                    f"4. 磁盘空间不足\n\n"
                    f"建议：\n"
                    f"- 关闭可能打开该文件的Excel程序\n"
                    f"- 检查文件是否完整（文件大小: {self.file_size / 1024 / 1024:.2f}MB）\n"
                    f"- 尝试将文件另存为新文件\n"
                    f"- 转换为CSV格式（推荐）\n"
                    f"原始错误: {str(e)}"
                )
            except Exception as e:
                error_type = type(e).__name__
                if 'EOF' in str(e) or 'corrupt' in str(e).lower() or 'truncated' in str(e).lower():
                    raise Exception(
                        f"Excel文件可能损坏或不完整（{error_type}）。\n"
                        f"文件大小: {self.file_size / 1024 / 1024:.2f}MB\n\n"
                        f"解决方案：\n"
                        f"1. 在Excel中打开文件，检查是否能正常打开\n"
                        f"2. 如果可以打开，尝试'另存为'新文件\n"
                        f"3. 将文件转换为CSV格式（推荐，处理速度更快）\n"
                        f"4. 检查文件是否完整下载/复制\n"
                        f"5. 检查磁盘空间是否充足\n"
                    )
                else:
                    raise Exception(f"读取Excel文件失败（{error_type}）: {str(e)}")
            
            total_rows = len(df)
            print(f"✅ 文件读取完成，共 {total_rows} 行数据")
            
            # 分批返回
            for i in range(0, total_rows, self.chunk_size):
                chunk_df = df.iloc[i:i+self.chunk_size].copy()
                
                # 不在这里调用callback，让上层处理器来调用
                # callback调用已经移到了data_processor_large.py中
                
                yield chunk_df
                
                # 释放内存
                del chunk_df
                gc.collect()
            
            # 释放原始DataFrame
            del df
            gc.collect()
            
        except MemoryError:
            raise MemoryError(
                f"内存不足，无法读取 {self.file_size / 1024 / 1024:.1f}MB 的Excel文件。\n"
                "建议：\n"
                "1. 将Excel转换为CSV格式（推荐）\n"
                "2. 增加系统内存\n"
                "3. 关闭其他占用内存的程序\n"
                "4. 使用更小的批次大小"
            )
        except Exception as e:
            # 如果已经是我们自定义的异常，直接抛出
            if "Excel文件" in str(e) or "文件可能损坏" in str(e):
                raise
            # 否则包装异常
            raise Exception(f"读取Excel文件失败: {str(e)}")
    
    def _verify_file_integrity(self) -> bool:
        """
        验证文件完整性
        :return: True if file appears valid, False otherwise
        """
        try:
            # 检查文件大小是否合理（至少应该大于0）
            if self.file_size == 0:
                return False
            
            # 对于Excel文件，检查文件头
            if self.file_ext == '.xlsx':
                # XLSX文件是ZIP格式，检查ZIP文件头
                with open(self.file_path, 'rb') as f:
                    header = f.read(4)
                    # ZIP文件头应该是 PK\x03\x04 或 PK\x05\x06
                    if header[:2] != b'PK':
                        return False
            elif self.file_ext == '.xls':
                # XLS文件（旧格式）检查OLE文件头
                with open(self.file_path, 'rb') as f:
                    header = f.read(8)
                    # OLE文件头
                    if header[:8] != b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
                        return False
            
            # 尝试打开文件检查是否可读
            try:
                if self.file_ext == '.xlsx':
                    import zipfile
                    with zipfile.ZipFile(self.file_path, 'r') as zf:
                        # 检查ZIP文件是否完整
                        zf.testzip()
                elif self.file_ext == '.xls':
                    # 对于XLS文件，只能检查文件头
                    pass
            except Exception:
                return False
            
            return True
            
        except Exception:
            return False
    
    def _estimate_csv_rows(self) -> int:
        """估算CSV文件的总行数（快速估算）"""
        try:
            # 读取前1000行来估算
            sample = pd.read_csv(self.file_path, nrows=1000, encoding='utf-8-sig')
            if len(sample) == 0:
                return 0
            
            # 估算平均行大小
            sample_size = self.file_path.stat().st_size
            avg_row_size = sample_size / len(sample) if len(sample) > 0 else 0
            
            # 总行数估算
            estimated = int(self.file_size / avg_row_size) if avg_row_size > 0 else 0
            return max(estimated, 1000)  # 至少返回1000
            
        except:
            # 如果估算失败，返回一个较大的数字
            return 100000
    
    def get_file_info(self) -> Dict:
        """获取文件基本信息"""
        info = {
            'file_name': self.file_path.name,
            'file_path': str(self.file_path),
            'file_size': self.file_size,
            'file_size_mb': self.file_size / 1024 / 1024,
            'file_ext': self.file_ext,
        }
        
        # 估算行数（仅CSV）
        if self.file_ext == '.csv':
            info['estimated_rows'] = self._estimate_csv_rows()
        
        return info
    
    def preview(self, n_rows: int = 5, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """预览文件前n行"""
        try:
            if self.file_ext == '.csv':
                return pd.read_csv(self.file_path, nrows=n_rows, encoding='utf-8-sig')
            else:
                engine = 'openpyxl' if self.file_ext == '.xlsx' else 'xlrd'
                return pd.read_excel(self.file_path, sheet_name=sheet_name, nrows=n_rows, engine=engine)
        except Exception as e:
            raise Exception(f"预览文件失败: {str(e)}")

