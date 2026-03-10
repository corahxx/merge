# data/data_loader.py - 数据入库器

import pandas as pd
import uuid
from datetime import date as date_type
from sqlalchemy import inspect, text
from typing import Dict, Optional, List
from utils.db_utils import create_db_engine


class DataLoader:
    """
    数据入库器
    负责将清洗后的数据导入到MySQL数据库
    """
    
    def __init__(self, table_name: str = 'table2509ev', verbose: bool = True):
        """
        初始化数据入库器
        :param table_name: 目标表名
        :param verbose: 是否显示详细日志
        """
        self.table_name = table_name
        self.verbose = verbose
        
        # 创建数据库连接（使用统一工具函数）
        self.engine = create_db_engine(echo=False)
        
        self.loading_stats = {
            'rows_loaded': 0,
            'rows_skipped': 0,
            'rows_updated': 0,
            'errors': []
        }
    
    def load(self, df: pd.DataFrame, if_exists: str = 'append', 
             method: str = 'multi', chunk_size: int = 1000) -> Dict:
        """
        加载数据到数据库
        :param df: 要加载的DataFrame
        :param if_exists: 如果表已存在如何处理 ('fail', 'replace', 'append')
        :param method: 插入方法 ('multi', 'callable')
        :param chunk_size: 分批插入的大小（会根据列数自动调整）
        :return: 加载结果统计
        """
        if df.empty:
            if self.verbose:
                print("⚠️  DataFrame为空，跳过加载")
            return self.loading_stats
        
        try:
            # 根据列数动态调整chunk_size，避免SQL参数过多
            # MySQL默认max_allowed_packet限制，参数数量建议不超过20000（更保守）
            # method='multi'会在一次INSERT中插入多行，参数数量 = 行数 × 列数
            num_columns = len(df.columns)
            max_params = 20000  # 更保守的阈值，避免超过MySQL参数限制
            max_safe_rows = max_params // max(num_columns, 1)  # 根据列数计算最大安全行数
            adjusted_chunk_size = min(chunk_size, max_safe_rows, 500)  # 同时限制最大500行
            
            # 确保至少为1
            adjusted_chunk_size = max(1, adjusted_chunk_size)
            
            if adjusted_chunk_size < chunk_size and self.verbose:
                print(f"⚠️  检测到 {num_columns} 列，将chunk_size从 {chunk_size} 调整为 {adjusted_chunk_size} 以避免SQL参数过多（参数限制: {max_params}，当前参数数: {adjusted_chunk_size * num_columns}）")
            
            chunk_size = adjusted_chunk_size
            # 检查表是否存在
            inspector = inspect(self.engine)
            table_exists = self.table_name in inspector.get_table_names()
            
            if not table_exists and if_exists == 'append':
                if self.verbose:
                    print(f"⚠️  表 {self.table_name} 不存在，将创建新表")
                if_exists = 'replace'
            
            # 自动生成UID字段（唯一标识ID）
            df = self._generate_uid(df)
            
            # 设置数据状态默认值
            df = self._set_default_status(df)
            
            # 清理日期字段：确保空字符串转换为None（NULL）
            df = self._clean_date_fields(df)
            
            # 分批插入数据
            total_rows = len(df)
            rows_inserted = 0
            
            # 记录插入前的行数（用于验证）
            from sqlalchemy import text
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM `{self.table_name}`"))
                    rows_before = result.scalar() or 0
            except:
                rows_before = 0
            
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                
                if chunk.empty:
                    if self.verbose:
                        print(f"⚠️  批次 {i//chunk_size + 1} 数据为空，跳过")
                    continue
                
                try:
                    # 根据列数和批次大小智能选择插入方法
                    # method='multi'会在一次INSERT中插入多行，参数数量 = 行数 × 列数
                    # 当参数数量可能过多时，使用逐行插入方法（更安全但稍慢）
                    estimated_params = len(chunk) * num_columns
                    use_multi_method = estimated_params <= 15000 and num_columns <= 30
                    actual_method = method if use_multi_method else None
                    
                    if actual_method != method and self.verbose and i == 0:
                        if num_columns > 30:
                            print(f"⚠️  列数过多（{num_columns}列），改用逐行插入方法以提高稳定性")
                        else:
                            print(f"⚠️  批次大小较大（{len(chunk)}行×{num_columns}列={estimated_params}参数），改用逐行插入方法以避免参数过多")
                    
                    # 注意：这里不传chunksize参数，因为我们已经手动分批了
                    # 如果传入chunksize，pandas会再次分批，可能导致参数计算错误
                    # 每批独立事务（文档：数据导入改造可行性分析）
                    with self.engine.begin() as conn:
                        chunk.to_sql(
                            name=self.table_name,
                            con=conn,
                            if_exists=if_exists if i == 0 else 'append',
                            index=False,
                            method=actual_method
                        )
                    rows_inserted += len(chunk)
                    
                    if self.verbose:
                        print(f"✅ 已插入 {rows_inserted}/{total_rows} 行数据...")
                    
                except Exception as e:
                    error_msg = f"批次 {i//chunk_size + 1} 插入失败: {str(e)}"
                    self.loading_stats['errors'].append(error_msg)
                    
                    if self.verbose:
                        print(f"❌ {error_msg}")
                        import traceback
                        traceback.print_exc()
                    
                    self.loading_stats['rows_skipped'] += len(chunk)
            
            # 验证实际插入的行数
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM `{self.table_name}`"))
                    rows_after = result.scalar() or 0
                actual_inserted = rows_after - rows_before
                
                if actual_inserted != rows_inserted and self.verbose:
                    print(f"⚠️  实际插入行数 ({actual_inserted}) 与预期 ({rows_inserted}) 不一致")
                
                # 使用实际插入的行数
                self.loading_stats['rows_loaded'] = max(actual_inserted, rows_inserted)
            except Exception as e:
                # 如果验证失败，使用计数值
                if self.verbose:
                    print(f"⚠️  无法验证插入行数: {str(e)}")
                self.loading_stats['rows_loaded'] = rows_inserted
            
            if self.verbose:
                self._print_stats()
            
            return self.loading_stats
            
        except Exception as e:
            error_msg = f"数据加载失败: {str(e)}"
            self.loading_stats['errors'].append(error_msg)
            
            if self.verbose:
                print(f"❌ {error_msg}")
            
            raise Exception(error_msg)
    
    def upsert(self, df: pd.DataFrame, unique_key: str = '充电桩编号') -> Dict:
        """
        更新或插入数据（如果记录存在则更新，不存在则插入）
        :param df: 要加载的DataFrame
        :param unique_key: 唯一键字段名
        :return: 加载结果统计
        """
        if df.empty:
            return self.loading_stats
        
        if unique_key not in df.columns:
            raise ValueError(f"DataFrame中不存在唯一键字段: {unique_key}")
        
        try:
            # 读取现有数据
            inspector = inspect(self.engine)
            if self.table_name not in inspector.get_table_names():
                # 表不存在，直接插入
                return self.load(df, if_exists='replace')
            
            existing_df = pd.read_sql_table(self.table_name, self.engine)
            
            # 找出需要更新和插入的数据
            if unique_key in existing_df.columns:
                existing_keys = set(existing_df[unique_key].astype(str))
                new_keys = set(df[unique_key].astype(str))
                
                # 需要更新的记录
                update_df = df[df[unique_key].astype(str).isin(existing_keys)]
                # 需要插入的记录
                insert_df = df[df[unique_key].astype(str).isin(new_keys - existing_keys)]
                
                if self.verbose:
                    print(f"📊 发现 {len(update_df)} 条记录需要更新")
                    print(f"📊 发现 {len(insert_df)} 条记录需要插入")
                
                # 先删除需要更新的记录（分批处理，避免参数过多）
                if not update_df.empty:
                    # 根据参数限制分批删除（IN子句限制）
                    batch_size = 1000  # 每批最多1000个key
                    keys_to_delete = update_df[unique_key].astype(str).tolist()
                    
                    for i in range(0, len(keys_to_delete), batch_size):
                        batch_keys = keys_to_delete[i:i+batch_size]
                        placeholders = ','.join([f':key{j}' for j in range(len(batch_keys))])
                        delete_sql = f"DELETE FROM `{self.table_name}` WHERE `{unique_key}` IN ({placeholders})"
                        params = {f'key{j}': key for j, key in enumerate(batch_keys)}
                        
                        with self.engine.connect() as conn:
                            conn.execute(text(delete_sql), params)
                            conn.commit()
                    
                    self.loading_stats['rows_updated'] = len(update_df)
                
                # 插入新数据和更新后的数据
                if not insert_df.empty or not update_df.empty:
                    combined_df = pd.concat([insert_df, update_df], ignore_index=True)
                    # 确保所有数据都有UID（load方法中也会生成，但这里提前生成更安全）
                    combined_df = self._generate_uid(combined_df)
                    # 清理日期字段
                    combined_df = self._clean_date_fields(combined_df)
                    self.load(combined_df, if_exists='append')
            else:
                # 如果没有唯一键，直接追加
                # 确保所有数据都有UID（load方法中也会生成，但这里提前生成更安全）
                df = self._generate_uid(df)
                # 清理日期字段
                df = self._clean_date_fields(df)
                self.load(df, if_exists='append')
            
            return self.loading_stats
            
        except Exception as e:
            error_msg = f"Upsert操作失败: {str(e)}"
            self.loading_stats['errors'].append(error_msg)
            raise Exception(error_msg)
    
    def get_table_schema(self) -> Dict:
        """
        获取目标表的字段信息
        :return: 字段信息字典
        """
        try:
            inspector = inspect(self.engine)
            if self.table_name not in inspector.get_table_names():
                return {}
            
            columns = inspector.get_columns(self.table_name)
            schema = {}
            for col in columns:
                schema[col['name']] = {
                    'type': str(col['type']),
                    'nullable': col['nullable'],
                    'default': col.get('default')
                }
            
            return schema
            
        except Exception as e:
            if self.verbose:
                print(f"⚠️  获取表结构失败: {str(e)}")
            return {}
    
    def _clean_date_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理日期字段，确保空字符串转换为None（NULL），斜杠替换为横线（只允许YYYY-MM-DD格式）
        :param df: 原始DataFrame
        :return: 清理后的DataFrame
        """
        df_copy = df.copy()
        
        # 获取表结构，找出所有DATE类型的字段
        try:
            schema = self.get_table_schema()
            if schema:
                for col_name, col_info in schema.items():
                    if col_name in df_copy.columns:
                        db_type = str(col_info.get('type', '')).upper()
                        # 如果是DATE类型字段（不包括DATETIME和TIMESTAMP）
                        if 'DATE' in db_type and 'DATETIME' not in db_type and 'TIMESTAMP' not in db_type:
                            # 清理日期字段：空字符串转None，斜杠替换为横线
                            # 统一处理：字符串做清理；datetime.date 直接通过；pd.Timestamp/NaT 转 date 或 None
                            def clean_date_value(x):
                                """清理单个日期值：空→None；字符串→清理；date→原样；Timestamp→date或None"""
                                if x is None:
                                    return None
                                if pd.isna(x):
                                    return None
                                # 已是 Python date，直接写入 DATE 列
                                if isinstance(x, date_type):
                                    return x
                                # pd.Timestamp / numpy datetime64 → date 或 None
                                if hasattr(x, 'date') and callable(getattr(x, 'date')):
                                    try:
                                        return x.date()
                                    except Exception:
                                        return None
                                # 字符串：空→None，斜杠→横线
                                if isinstance(x, str):
                                    x_stripped = x.strip()
                                    if x_stripped == '' or x_stripped.lower() in ['nan', 'none', 'null', 'nat']:
                                        return None
                                    if '/' in x_stripped:
                                        x_stripped = x_stripped.replace('/', '-')
                                        if self.verbose:
                                            print(f"⚠️  日期字段 '{col_name}' 中的斜杠已替换为横线: {x} -> {x_stripped}")
                                    return x_stripped
                                # numpy.datetime64 等：尝试通过 pd.Timestamp 转为 date
                                try:
                                    ts = pd.Timestamp(x)
                                    return None if pd.isna(ts) else ts.date()
                                except Exception:
                                    pass
                                return None

                            df_copy[col_name] = df_copy[col_name].apply(clean_date_value)
                            
                            # 确保pd.NA也被转换为None
                            df_copy[col_name] = df_copy[col_name].where(df_copy[col_name].notna(), None)
        except Exception as e:
            if self.verbose:
                print(f"⚠️  清理日期字段时出错: {str(e)}")
        
        return df_copy
    
    def _set_default_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        设置数据状态默认值
        - is_active: 1 (活跃)
        - data_status: 0 (正常)
        
        data_status 状态代码:
          0 = normal (正常)
          1 = duplicate (重复)
          2 = suspect (疑似)
          3 = fixed (已修复)
          4 = deleted (已删除)
        """
        df_copy = df.copy()
        
        # 设置 is_active 默认值为 1
        if 'is_active' not in df_copy.columns:
            df_copy['is_active'] = 1
        else:
            # 填充空值为 1
            df_copy['is_active'] = df_copy['is_active'].fillna(1).astype(int)
        
        # 设置 data_status 默认值为 0 (正常)
        if 'data_status' not in df_copy.columns:
            df_copy['data_status'] = 0
        else:
            # 填充空值为 0
            df_copy['data_status'] = df_copy['data_status'].fillna(0).astype(int)
        
        if self.verbose:
            print(f"✅ 已设置数据状态默认值: is_active=1, data_status=0")
        
        return df_copy
    
    def _generate_uid(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        为DataFrame生成唯一标识ID并填充到UID字段
        :param df: 原始DataFrame
        :return: 添加了UID字段的DataFrame
        """
        df_copy = df.copy()
        
        # 检查是否有UID列
        if 'UID' in df_copy.columns:
            # 如果UID列存在，为空的或无效的值生成新的UUID
            # 检查哪些行需要生成UID（空值、None、空字符串等）
            mask = (
                df_copy['UID'].isna() | 
                (df_copy['UID'].astype(str).str.strip() == '') |
                (df_copy['UID'].astype(str) == 'None')
            )
            if mask.any():
                # 为需要生成UID的行创建UUID（字符串格式）
                df_copy.loc[mask, 'UID'] = [str(uuid.uuid4()) for _ in range(mask.sum())]
                if self.verbose:
                    print(f"✅ 为 {mask.sum()} 行数据生成了唯一标识ID (UID)")
        else:
            # 如果UID列不存在，创建新列并为所有行生成UUID
            df_copy['UID'] = [str(uuid.uuid4()) for _ in range(len(df_copy))]
            if self.verbose:
                print(f"✅ 为所有 {len(df_copy)} 行数据生成了唯一标识ID (UID)")
        
        return df_copy
    
    def _print_stats(self):
        """打印加载统计信息"""
        print("\n" + "="*50)
        print("📤 数据加载统计")
        print("="*50)
        print(f"成功加载: {self.loading_stats['rows_loaded']} 行")
        print(f"跳过: {self.loading_stats['rows_skipped']} 行")
        print(f"更新: {self.loading_stats['rows_updated']} 行")
        if self.loading_stats['errors']:
            print(f"错误: {len(self.loading_stats['errors'])} 个")
            for error in self.loading_stats['errors'][:5]:  # 只显示前5个错误
                print(f"  - {error}")
        print("="*50 + "\n")
    
    def get_stats(self) -> Dict:
        """获取加载统计信息"""
        return self.loading_stats.copy()

