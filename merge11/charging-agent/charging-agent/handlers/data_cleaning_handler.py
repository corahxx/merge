# handlers/data_cleaning_handler.py - 数据清洗处理器

import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import pandas as pd
from sqlalchemy import text
from utils.db_utils import get_shared_engine
from data.error_handler import ErrorHandler

# data_status 状态码常量
class DataStatus:
    """数据状态码定义"""
    NORMAL = 0      # 正常
    DUPLICATE = 1   # 重复
    SUSPECT = 2     # 疑似问题
    FIXED = 3       # 已修复
    DELETED = 4     # 已删除
    
    @classmethod
    def get_name(cls, code: int) -> str:
        """获取状态名称"""
        names = {0: '正常', 1: '重复', 2: '疑似', 3: '已修复', 4: '已删除'}
        return names.get(code, '未知')


class AddressParser:
    """地址解析器 - 从场站地址中提取省市区"""
    
    # 直辖市
    DIRECT_CITIES = ['北京', '上海', '天津', '重庆']
    
    # 省份后缀
    PROVINCE_SUFFIXES = ['省', '自治区', '特别行政区']
    
    # 城市后缀
    CITY_SUFFIXES = ['市', '自治州', '地区', '盟']
    
    # 区县后缀
    DISTRICT_SUFFIXES = ['区', '县', '市', '旗']
    
    def parse(self, address: str) -> Dict:
        """
        解析地址，提取省份、城市、区县
        
        :param address: 原始地址字符串
        :return: {'province': '', 'city': '', 'district': '', 'confidence': 0.0}
        """
        result = {
            'province': None,
            'city': None,
            'district': None,
            'confidence': 0.0
        }
        
        if not address or not isinstance(address, str):
            return result
        
        address = address.strip()
        
        # 1. 检查直辖市
        for city in self.DIRECT_CITIES:
            if city in address:
                result['province'] = f"{city}市"
                result['city'] = f"{city}市"
                # 提取区
                district = self._extract_district(address)
                if district:
                    result['district'] = district
                result['confidence'] = 0.9 if district else 0.7
                return result
        
        # 2. 提取省份
        province = self._extract_province(address)
        if province:
            result['province'] = province
            result['confidence'] += 0.3
        
        # 3. 提取城市
        city = self._extract_city(address)
        if city:
            result['city'] = city
            result['confidence'] += 0.3
        
        # 4. 提取区县
        district = self._extract_district(address)
        if district:
            result['district'] = district
            result['confidence'] += 0.3
        
        return result
    
    def _extract_province(self, address: str) -> Optional[str]:
        """提取省份"""
        # 匹配省份模式
        pattern = r'([\u4e00-\u9fa5]{2,}(?:省|自治区))'
        match = re.search(pattern, address)
        if match:
            return match.group(1)
        return None
    
    def _extract_city(self, address: str) -> Optional[str]:
        """提取城市"""
        # 跳过直辖市
        for dc in self.DIRECT_CITIES:
            if f"{dc}市" in address:
                return None
        
        # 匹配城市模式
        pattern = r'([\u4e00-\u9fa5]{2,}(?:市|自治州|地区|盟))'
        match = re.search(pattern, address)
        if match:
            city = match.group(1)
            # 排除区级市
            if not any(city.endswith(suffix) for suffix in ['县级市']):
                return city
        return None
    
    def _extract_district(self, address: str) -> Optional[str]:
        """提取区县"""
        # 匹配区县模式，优先匹配"区"
        patterns = [
            r'([\u4e00-\u9fa5]{2,}区)',  # 优先匹配区
            r'([\u4e00-\u9fa5]{2,}县)',
            r'([\u4e00-\u9fa5]{2,}旗)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, address)
            if match:
                district = match.group(1)
                # 排除"地区"
                if district != '地区' and not district.endswith('地区'):
                    return district
        return None


class DataCleaningHandler:
    """数据清洗处理器（使用共享的PandasDataService）"""
    
    def __init__(self, table_name: str = 'evdata'):
        self.table_name = table_name
        self._engine = None
        self.address_parser = AddressParser()
        
        # 使用共享的PandasDataService
        self._pandas_service = None
        
        # 重复数据结果缓存
        self._full_dup_cache = None
        self._high_suspect_cache = None
    
    @property
    def engine(self):
        """获取数据库引擎"""
        if self._engine is None:
            self._engine = get_shared_engine()
        return self._engine
    
    @property
    def pandas_service(self):
        """获取共享的Pandas数据服务"""
        if self._pandas_service is None:
            try:
                from services.pandas_data_service import PandasDataService
                self._pandas_service = PandasDataService.get_instance()
            except ImportError:
                self._pandas_service = None
        return self._pandas_service
    
    # ==================== Pandas 全量加载 ====================
    
    def _load_data_to_memory(self, progress_callback=None) -> pd.DataFrame:
        """
        加载全量数据到内存（使用共享的PandasDataService）
        首次加载约30秒，后续使用缓存（毫秒级）
        """
        import time
        
        # 使用共享的PandasDataService
        if self.pandas_service:
            # 检查缓存是否已加载
            if self.pandas_service.is_loaded():
                if progress_callback:
                    progress_callback(100, 100, 0, "Using shared data cache")
                return self.pandas_service.get_dataframe()
            
            # 首次加载
            def pandas_progress(current, total, message):
                if progress_callback:
                    elapsed = 0  # PandasDataService内部管理时间
                    progress_callback(current, total, elapsed, message)
            
            start = time.time()
            df = self.pandas_service.get_dataframe(progress_callback=pandas_progress)
            elapsed = time.time() - start
            
            if progress_callback:
                mem_usage = df.memory_usage(deep=True).sum() / (1024**3)
                progress_callback(100, 100, elapsed, 
                    f"Loaded {len(df):,} records, memory: {mem_usage:.2f}GB")
            
            return df
        
        # 降级：独立加载（不使用共享服务）
        start = time.time()
        
        if progress_callback:
            progress_callback(0, 100, 0, "Loading data to memory...")
        
        query = """
            SELECT 
                UID, `充电桩编号`, `运营商名称`, 
                `省份_中文`, `城市_中文`, `区县_中文`, 
                `充电站位置`, `额定功率`, `充电桩类型_转换`,
                `所属充电站编号`
            FROM evdata
            WHERE is_active = 1
        """
        
        df = pd.read_sql(text(query), self.engine)
        
        elapsed = time.time() - start
        mem_usage = df.memory_usage(deep=True).sum() / (1024**3)
        
        if progress_callback:
            progress_callback(100, 100, elapsed, 
                f"Loaded {len(df):,} records, memory: {mem_usage:.2f}GB")
        
        return df
    
    # ==================== 重复数据检测 ====================
    
    # 完全重复比对字段（除UID外）
    FULL_DUPLICATE_COLS = [
        '充电桩编号', '运营商名称', '省份_中文', '城市_中文',
        '区县_中文', '充电站位置', '充电桩类型_转换', '额定功率', '所属充电站编号'
    ]
    
    def scan_full_duplicates(self, page: int = 1, page_size: int = 50) -> Dict:
        """
        扫描完全重复数据（除UID外所有字段相同）- Pandas全量模式
        首次加载数据约60秒，后续扫描秒级完成
        """
        try:
            # 构建缓存（如果没有）
            if self._full_dup_cache is None:
                self._build_full_duplicate_cache()
            
            # 分页返回
            total = len(self._full_dup_cache) if self._full_dup_cache else 0
            offset = (page - 1) * page_size
            data = self._full_dup_cache[offset:offset + page_size] if self._full_dup_cache else []
            
            return {
                'success': True,
                'data': data,
                'total': total,
                'page': page,
                'page_size': page_size
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "扫描完全重复数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
    
    def _build_full_duplicate_cache(self, progress_callback=None):
        """
        构建完全重复数据缓存 - Pandas全量加载模式
        32GB内存环境下，首次加载约60秒，重复检测约1-2秒
        """
        import time
        start_time = time.time()
        
        # 第1步：加载全量数据到内存
        df = self._load_data_to_memory(progress_callback)
        
        if df.empty:
            self._full_dup_cache = []
            return
        
        if progress_callback:
            progress_callback(50, 100, time.time() - start_time, "Detecting full duplicates...")
        
        # 第2步：Pandas内存检测（秒级完成）
        compare_cols = self.FULL_DUPLICATE_COLS
        
        # 找出在所有比对字段上重复的记录
        df['is_dup'] = df.duplicated(subset=compare_cols, keep=False)
        full_dups = df[df['is_dup']].copy()
        
        if full_dups.empty:
            self._full_dup_cache = []
            if progress_callback:
                progress_callback(100, 100, time.time() - start_time, "No full duplicates found")
            return
        
        if progress_callback:
            progress_callback(70, 100, time.time() - start_time, f"Found {len(full_dups)} duplicate records, grouping...")
        
        # 第3步：按组聚合结果
        result = []
        
        # 使用 groupby 替代手动分组（更高效）
        grouped = full_dups.groupby(compare_cols, dropna=False)
        
        for group_key, group in grouped:
            if len(group) > 1:
                first_row = group.iloc[0]
                result.append({
                    '充电桩编号': first_row['充电桩编号'],
                    '运营商名称': first_row['运营商名称'],
                    '省份_中文': first_row['省份_中文'],
                    '城市_中文': first_row['城市_中文'],
                    '区县_中文': first_row['区县_中文'],
                    '充电站位置': first_row['充电站位置'],
                    '充电桩类型_转换': first_row['充电桩类型_转换'],
                    '额定功率': first_row['额定功率'],
                    '所属充电站编号': first_row['所属充电站编号'],
                    'duplicate_count': len(group),
                    'uid_list': ','.join(group['UID'].astype(str).tolist())
                })
        
        # 按重复数量降序排序
        result.sort(key=lambda x: x['duplicate_count'], reverse=True)
        self._full_dup_cache = result
        
        elapsed = time.time() - start_time
        if progress_callback:
            progress_callback(100, 100, elapsed, f"Done: found {len(result)} duplicate groups in {elapsed:.1f}s")
    
    def clear_duplicate_cache(self):
        """清除所有缓存（数据变更后调用）"""
        self._full_dup_cache = None
        self._high_suspect_cache = None
        self._data_cache = None
        self._cache_time = None
    
    # 高度疑似重复比对字段
    HIGH_SUSPECT_COLS = ['充电桩编号', '运营商名称', '充电站位置', '额定功率']
    
    def scan_high_suspect_duplicates(self, page: int = 1, page_size: int = 50) -> Dict:
        """
        扫描高度疑似重复数据（充电桩编号+地址+运营商+功率相同）- Pandas全量模式
        """
        try:
            if self._high_suspect_cache is None:
                self._build_high_suspect_cache()
            
            total = len(self._high_suspect_cache) if self._high_suspect_cache else 0
            offset = (page - 1) * page_size
            data = self._high_suspect_cache[offset:offset + page_size] if self._high_suspect_cache else []
            
            return {
                'success': True,
                'data': data,
                'total': total,
                'page': page,
                'page_size': page_size
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "扫描高度疑似重复数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
    
    def _build_high_suspect_cache(self, progress_callback=None):
        """
        构建高度疑似重复数据缓存 - Pandas全量加载模式
        """
        import time
        start_time = time.time()
        
        # 第1步：加载全量数据到内存（复用缓存）
        df = self._load_data_to_memory(progress_callback)
        
        if df.empty:
            self._high_suspect_cache = []
            return
        
        if progress_callback:
            progress_callback(50, 100, time.time() - start_time, "Detecting high-suspect duplicates...")
        
        # 第2步：Pandas内存检测
        compare_cols = self.HIGH_SUSPECT_COLS
        
        df['is_dup'] = df.duplicated(subset=compare_cols, keep=False)
        high_suspects = df[df['is_dup']].copy()
        
        if high_suspects.empty:
            self._high_suspect_cache = []
            if progress_callback:
                progress_callback(100, 100, time.time() - start_time, "No high-suspect duplicates found")
            return
        
        if progress_callback:
            progress_callback(70, 100, time.time() - start_time, f"Found {len(high_suspects)} suspect records, grouping...")
        
        # 第3步：按组聚合
        result = []
        grouped = high_suspects.groupby(compare_cols, dropna=False)
        
        for _, group in grouped:
            if len(group) > 1:
                first_row = group.iloc[0]
                result.append({
                    '充电桩编号': first_row['充电桩编号'],
                    '运营商名称': first_row['运营商名称'],
                    '充电站位置': first_row['充电站位置'],
                    '额定功率': first_row['额定功率'],
                    'duplicate_count': len(group),
                    'uid_list': ','.join(group['UID'].astype(str).tolist()),
                    'provinces': ','.join(group['省份_中文'].dropna().unique().astype(str).tolist()),
                    'cities': ','.join(group['城市_中文'].dropna().unique().astype(str).tolist())
                })
        
        result.sort(key=lambda x: x['duplicate_count'], reverse=True)
        self._high_suspect_cache = result
        
        elapsed = time.time() - start_time
        if progress_callback:
            progress_callback(100, 100, elapsed, f"Done: found {len(result)} suspect groups in {elapsed:.1f}s")
    
    def get_duplicate_records_by_uids(self, uid_list: str) -> List[Dict]:
        """
        根据UID列表获取重复记录详情
        """
        try:
            uids = [uid.strip() for uid in uid_list.split(',')]
            placeholders = ','.join([f':uid{i}' for i in range(len(uids))])
            
            query = text(f"""
                SELECT 
                    UID,
                    `充电桩编号`,
                    `运营商名称`,
                    `省份_中文`,
                    `城市_中文`,
                    `区县_中文`,
                    `充电站位置`,
                    `充电桩类型_转换`,
                    `额定功率`,
                    `所属充电站编号`,
                    `充电站投入使用时间`,
                    data_status,
                    created_at
                FROM evdata
                WHERE UID IN ({placeholders})
                ORDER BY created_at
            """)
            
            params = {f'uid{i}': uid for i, uid in enumerate(uids)}
            df = pd.read_sql(query, self.engine, params=params)
            
            return df.to_dict('records')
        except Exception as e:
            return []
    
    # ==================== 地址一致性检测 ====================
    
    def get_address_consistency_summary(self, top_n: int = 10) -> Dict:
        """
        获取地址一致性概览（数据库侧统计）
        
        统计维度：
        - 总活跃数据量
        - 有地址数据量
        - 地址为空/缺失数据量
        - 省份不匹配 / 城市不匹配 / 总不匹配（省或市任一不匹配）
        - 不匹配 Top 省份/城市
        """
        try:
            with self.engine.connect() as conn:
                total_active = conn.execute(text("""
                    SELECT COUNT(*) FROM evdata WHERE is_active = 1
                """)).scalar() or 0
                
                total_with_address = conn.execute(text("""
                    SELECT COUNT(*) FROM evdata
                    WHERE is_active = 1
                      AND `充电站位置` IS NOT NULL
                      AND `充电站位置` != ''
                """)).scalar() or 0
                
                total_address_empty = total_active - total_with_address
                
                # 与 scan_address_issues 的条件保持一致
                province_mismatch = conn.execute(text("""
                    SELECT COUNT(*) FROM evdata
                    WHERE is_active = 1
                      AND `充电站位置` IS NOT NULL
                      AND `充电站位置` != ''
                      AND `省份_中文` IS NOT NULL AND `省份_中文` != ''
                      AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%')
                """)).scalar() or 0
                
                city_mismatch = conn.execute(text("""
                    SELECT COUNT(*) FROM evdata
                    WHERE is_active = 1
                      AND `充电站位置` IS NOT NULL
                      AND `充电站位置` != ''
                      AND `城市_中文` IS NOT NULL AND `城市_中文` != ''
                      AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%')
                """)).scalar() or 0
                
                total_mismatch = conn.execute(text("""
                    SELECT COUNT(*) FROM evdata
                    WHERE is_active = 1
                      AND `充电站位置` IS NOT NULL
                      AND `充电站位置` != ''
                      AND (
                        (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                        OR
                        (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                      )
                """)).scalar() or 0
                
                # Top 省份
                top_province_rows = conn.execute(text(f"""
                    SELECT `省份_中文` AS province, COUNT(*) AS cnt
                    FROM evdata
                    WHERE is_active = 1
                      AND `充电站位置` IS NOT NULL
                      AND `充电站位置` != ''
                      AND (
                        (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                        OR
                        (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                      )
                    GROUP BY `省份_中文`
                    ORDER BY cnt DESC
                    LIMIT {int(top_n)}
                """)).fetchall()
                
                # Top 城市
                top_city_rows = conn.execute(text(f"""
                    SELECT `城市_中文` AS city, COUNT(*) AS cnt
                    FROM evdata
                    WHERE is_active = 1
                      AND `充电站位置` IS NOT NULL
                      AND `充电站位置` != ''
                      AND (
                        (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                        OR
                        (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                      )
                    GROUP BY `城市_中文`
                    ORDER BY cnt DESC
                    LIMIT {int(top_n)}
                """)).fetchall()
                
                top_provinces = [{'province': r[0] or '未知', 'count': int(r[1] or 0)} for r in top_province_rows]
                top_cities = [{'city': r[0] or '未知', 'count': int(r[1] or 0)} for r in top_city_rows]
                
                mismatch_rate = (total_mismatch / total_with_address) if total_with_address else 0.0
                
                return {
                    'success': True,
                    'total_active': int(total_active),
                    'total_with_address': int(total_with_address),
                    'total_address_empty': int(total_address_empty),
                    'province_mismatch': int(province_mismatch),
                    'city_mismatch': int(city_mismatch),
                    'total_mismatch': int(total_mismatch),
                    'mismatch_rate': float(mismatch_rate),
                    'top_provinces': top_provinces,
                    'top_cities': top_cities,
                    'top_n': int(top_n),
                }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "获取地址一致性概览")
            return {'success': False, 'error': error_info['error_message']}
    
    def scan_address_issues(self, page: int = 1, page_size: int = 50) -> Dict:
        """
        扫描地址不一致的数据
        """
        try:
            # 查询地址与省市区不匹配的记录
            query = text("""
                SELECT 
                    UID,
                    `充电桩编号`,
                    `运营商名称`,
                    `省份_中文`,
                    `城市_中文`,
                    `区县_中文`,
                    `充电站位置`
                FROM evdata
                WHERE is_active = 1
                AND `充电站位置` IS NOT NULL
                AND `充电站位置` != ''
                AND (
                    (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                     AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                    OR
                    (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                     AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                )
                LIMIT :limit OFFSET :offset
            """)
            
            offset = (page - 1) * page_size
            df = pd.read_sql(query, self.engine, params={'limit': page_size, 'offset': offset})
            
            # 为每条记录添加解析建议
            records = df.to_dict('records')
            for record in records:
                parsed = self.address_parser.parse(record.get('充电站位置', ''))
                record['parsed_province'] = parsed['province']
                record['parsed_city'] = parsed['city']
                record['parsed_district'] = parsed['district']
                record['parse_confidence'] = parsed['confidence']
            
            # 获取总数
            count_query = text("""
                SELECT COUNT(*) as total
                FROM evdata
                WHERE is_active = 1
                AND `充电站位置` IS NOT NULL
                AND `充电站位置` != ''
                AND (
                    (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                     AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                    OR
                    (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                     AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                )
            """)
            total_df = pd.read_sql(count_query, self.engine)
            total = int(total_df.iloc[0]['total']) if not total_df.empty else 0
            
            return {
                'success': True,
                'data': records,
                'total': total,
                'page': page,
                'page_size': page_size
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "扫描地址不一致数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
    
    # ==================== 数据标记操作 ====================
    
    def soft_delete(self, uid: str, note: str, operator: str) -> Dict:
        """
        软删除：标记为deleted，设置is_active=0
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    UPDATE evdata 
                    SET is_active = 0,
                        data_status = 4,
                        status_note = :note,
                        cleaned_at = NOW(),
                        cleaned_by = :operator
                    WHERE UID = :uid
                """)
                result = conn.execute(query, {'uid': uid, 'note': note, 'operator': operator})
                conn.commit()
                
                # 数据变更后清除缓存
                self.clear_duplicate_cache()
                
                return {'success': True, 'affected_rows': result.rowcount}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "软删除")
            return {'success': False, 'error': error_info['error_message']}
    
    def batch_soft_delete(self, uids: List[str], note: str, operator: str) -> Dict:
        """
        批量软删除：标记为deleted，设置is_active=0
        """
        try:
            with self.engine.connect() as conn:
                total_affected = 0
                for uid in uids:
                    query = text("""
                        UPDATE evdata 
                        SET is_active = 0,
                            data_status = 4,
                            status_note = :note,
                            cleaned_at = NOW(),
                            cleaned_by = :operator
                        WHERE UID = :uid
                    """)
                    result = conn.execute(query, {'uid': uid, 'note': note, 'operator': operator})
                    total_affected += result.rowcount
                conn.commit()
                
                # 数据变更后清除缓存
                self.clear_duplicate_cache()
                
                return {'success': True, 'affected_rows': total_affected}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "批量软删除")
            return {'success': False, 'error': error_info['error_message']}
    
    def batch_soft_delete_optimized(self, uids: List[str], note: str, operator: str,
                                    batch_size: int = 500, 
                                    progress_callback: Optional[callable] = None) -> Dict:
        """
        优化的批量软删除（分批处理 + 批量UPDATE）
        
        :param uids: 要删除的UID列表
        :param note: 删除原因备注
        :param operator: 操作人
        :param batch_size: 每批处理数量（默认500）
        :param progress_callback: 进度回调 callback(processed, total, message)
        :return: 处理结果字典
        """
        if not uids:
            return {'success': True, 'affected_rows': 0}
        
        total = len(uids)
        total_affected = 0
        failed_batches = []
        
        try:
            with self.engine.connect() as conn:
                for i in range(0, total, batch_size):
                    batch_uids = uids[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    
                    try:
                        # 使用 IN 子句批量更新
                        placeholders = ','.join([f':uid{j}' for j in range(len(batch_uids))])
                        query = text(f"""
                            UPDATE evdata 
                            SET is_active = 0,
                                data_status = 4,
                                status_note = :note,
                                cleaned_at = NOW(),
                                cleaned_by = :operator
                            WHERE UID IN ({placeholders})
                        """)
                        
                        params = {'note': note, 'operator': operator}
                        params.update({f'uid{j}': uid for j, uid in enumerate(batch_uids)})
                        
                        result = conn.execute(query, params)
                        total_affected += result.rowcount
                        conn.commit()  # 每批提交
                        
                    except Exception as batch_error:
                        # 单批失败，记录并继续
                        failed_batches.append({
                            'batch': batch_num,
                            'count': len(batch_uids),
                            'error': str(batch_error)
                        })
                        conn.rollback()
                    
                    # 进度回调
                    if progress_callback:
                        processed = min(i + batch_size, total)
                        progress_callback(processed, total, f"已处理 {processed}/{total} 条")
                
                # 数据变更后清除缓存
                self.clear_duplicate_cache()
                
                # 清除Pandas缓存
                if self.pandas_service:
                    self.pandas_service.clear_cache()
                
                result = {
                    'success': len(failed_batches) == 0,
                    'affected_rows': total_affected,
                    'total_requested': total,
                    'failed_batches': failed_batches
                }
                
                if failed_batches:
                    result['error'] = f"{len(failed_batches)} 个批次处理失败"
                
                return result
                
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "优化批量软删除")
            return {
                'success': False, 
                'error': error_info['error_message'],
                'affected_rows': total_affected
            }
    
    # 完全重复的比对字段（与检测逻辑一致）
    DUPLICATE_GROUP_COLS = [
        '充电桩编号', '运营商名称', '省份_中文', '城市_中文',
        '区县_中文', '充电站位置', '充电桩类型_转换', '额定功率', '所属充电站编号'
    ]
    
    def delete_all_duplicates_fast(self, keep_strategy: str = 'first',
                                    operator: str = 'system',
                                    progress_callback: Optional[callable] = None) -> Dict:
        """
        【高性能】一键删除所有重复数据 - 两阶段处理
        
        删除逻辑与检测逻辑一致，基于9个字段判定重复：
        充电桩编号, 运营商名称, 省份_中文, 城市_中文, 区县_中文, 
        充电站位置, 充电桩类型_转换, 额定功率, 所属充电站编号
        
        两阶段策略：
        1. 阶段一：查询所有要删除的UID（只读，不加锁）
        2. 阶段二：分批更新这些UID（小批量更新，避免锁超时）
        
        :param keep_strategy: 保留策略 - 'first' 保留最小UID, 'last' 保留最大UID
        :param operator: 操作人
        :param progress_callback: 进度回调 callback(current, total, message)
        :return: 处理结果字典
        """
        try:
            if keep_strategy == 'last':
                agg_func = 'MAX'
            else:
                agg_func = 'MIN'
            
            note = f"批量清理重复数据，保留策略:{keep_strategy}"
            
            # 构建GROUP BY字段列表
            group_cols = ', '.join([f'`{col}`' for col in self.DUPLICATE_GROUP_COLS])
            
            # ========== 阶段一：查询所有要删除的UID ==========
            if progress_callback:
                progress_callback(0, 100, "阶段1: 正在识别重复数据...")
            
            with self.engine.connect() as conn:
                # 使用MD5哈希简化分组，避免9字段JOIN
                # 查询所有要删除的UID（保留每组的第一条/最后一条）
                find_duplicates_sql = text(f"""
                    SELECT e.UID
                    FROM evdata e
                    INNER JOIN (
                        SELECT 
                            MD5(CONCAT_WS('|', 
                                IFNULL(`充电桩编号`, ''),
                                IFNULL(`运营商名称`, ''),
                                IFNULL(`省份_中文`, ''),
                                IFNULL(`城市_中文`, ''),
                                IFNULL(`区县_中文`, ''),
                                IFNULL(`充电站位置`, ''),
                                IFNULL(`充电桩类型_转换`, ''),
                                IFNULL(`额定功率`, ''),
                                IFNULL(`所属充电站编号`, '')
                            )) as hash_key,
                            {agg_func}(UID) as keep_uid
                        FROM evdata
                        WHERE is_active = 1
                        GROUP BY hash_key
                        HAVING COUNT(*) > 1
                    ) keep ON MD5(CONCAT_WS('|', 
                        IFNULL(e.`充电桩编号`, ''),
                        IFNULL(e.`运营商名称`, ''),
                        IFNULL(e.`省份_中文`, ''),
                        IFNULL(e.`城市_中文`, ''),
                        IFNULL(e.`区县_中文`, ''),
                        IFNULL(e.`充电站位置`, ''),
                        IFNULL(e.`充电桩类型_转换`, ''),
                        IFNULL(e.`额定功率`, ''),
                        IFNULL(e.`所属充电站编号`, '')
                    )) = keep.hash_key
                    WHERE e.is_active = 1 AND e.UID != keep.keep_uid
                """)
                
                result = conn.execute(find_duplicates_sql)
                delete_uids = [row[0] for row in result.fetchall()]
            
            total_to_delete = len(delete_uids)
            
            if total_to_delete == 0:
                if progress_callback:
                    progress_callback(100, 100, "没有发现重复数据")
                return {
                    'success': True,
                    'affected_rows': 0,
                    'method': 'two_phase_hash'
                }
            
            # ========== 阶段二：分批更新UID ==========
            if progress_callback:
                progress_callback(10, 100, f"阶段2: 正在删除 {total_to_delete:,} 条重复数据...")
            
            batch_size = 500  # 每批500条
            total_affected = 0
            
            with self.engine.connect() as conn:
                for i in range(0, total_to_delete, batch_size):
                    batch_uids = delete_uids[i:i + batch_size]
                    
                    # 构建IN子句的占位符
                    placeholders = ', '.join([f':uid_{j}' for j in range(len(batch_uids))])
                    
                    update_sql = text(f"""
                        UPDATE evdata 
                        SET is_active = 0,
                            data_status = 4,
                            status_note = :note,
                            cleaned_at = NOW(),
                            cleaned_by = :operator
                        WHERE UID IN ({placeholders})
                        AND is_active = 1
                    """)
                    
                    # 构建参数字典
                    params = {
                        'note': note,
                        'operator': operator
                    }
                    for j, uid in enumerate(batch_uids):
                        params[f'uid_{j}'] = uid
                    
                    result = conn.execute(update_sql, params)
                    total_affected += result.rowcount
                    conn.commit()
                    
                    # 更新进度（10%-100%）
                    if progress_callback:
                        pct = 10 + int(90 * (i + batch_size) / total_to_delete)
                        progress_callback(min(pct, 100), 100, 
                            f"已处理 {min(i + batch_size, total_to_delete):,}/{total_to_delete:,} 条")
            
            if progress_callback:
                progress_callback(100, 100, "完成")
            
            # 清除缓存
            self.clear_duplicate_cache()
            if self.pandas_service:
                self.pandas_service.clear_cache()
            
            return {
                'success': True,
                'affected_rows': total_affected,
                'total_identified': total_to_delete,
                'method': 'two_phase_hash'
            }
                
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "快速批量删除重复数据")
            return {'success': False, 'error': error_info['error_message']}
    
    def delete_all_duplicates(self, duplicate_groups: List[Dict], 
                             keep_strategy: str = 'first',
                             operator: str = 'system',
                             progress_callback: Optional[callable] = None) -> Dict:
        """
        一键删除所有重复数据（基于扫描结果）
        
        :param duplicate_groups: 重复数据组列表（来自扫描结果）
        :param keep_strategy: 保留策略 - 'first' 保留第一条, 'last' 保留最后一条
        :param operator: 操作人
        :param progress_callback: 进度回调 callback(processed, total, message)
        :return: 处理结果字典
        """
        if not duplicate_groups:
            return {'success': True, 'affected_rows': 0, 'groups_processed': 0}
        
        all_delete_uids = []
        keep_uids = []
        
        # 从每组中提取要删除的UID
        for group in duplicate_groups:
            uid_list_str = group.get('uid_list', '')
            if not uid_list_str:
                continue
            
            uid_list = [uid.strip() for uid in uid_list_str.split(',') if uid.strip()]
            if len(uid_list) <= 1:
                continue
            
            # 根据策略选择保留哪一条
            if keep_strategy == 'last':
                keep_uid = uid_list[-1]
            else:  # 默认 'first'
                keep_uid = uid_list[0]
            
            keep_uids.append(keep_uid)
            delete_uids = [u for u in uid_list if u != keep_uid]
            all_delete_uids.extend(delete_uids)
        
        if not all_delete_uids:
            return {
                'success': True, 
                'affected_rows': 0, 
                'groups_processed': len(duplicate_groups),
                'message': '没有需要删除的记录'
            }
        
        # 执行批量删除
        note = f"批量清理重复数据，保留策略:{keep_strategy}，保留{len(keep_uids)}条"
        result = self.batch_soft_delete_optimized(
            all_delete_uids,
            note=note,
            operator=operator,
            progress_callback=progress_callback
        )
        
        result['groups_processed'] = len(duplicate_groups)
        result['keep_count'] = len(keep_uids)
        result['delete_requested'] = len(all_delete_uids)
        
        return result
    
    def mark_as_duplicate(self, uid: str, group_id: int, note: str, operator: str) -> Dict:
        """
        标记为重复（保留显示）
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    UPDATE evdata 
                    SET data_status = 1,
                        duplicate_group_id = :group_id,
                        status_note = :note,
                        cleaned_at = NOW(),
                        cleaned_by = :operator
                    WHERE UID = :uid
                """)
                result = conn.execute(query, {
                    'uid': uid, 
                    'group_id': group_id, 
                    'note': note, 
                    'operator': operator
                })
                conn.commit()
                
                return {'success': True, 'affected_rows': result.rowcount}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "标记重复")
            return {'success': False, 'error': error_info['error_message']}
    
    def mark_as_normal(self, uid: str, operator: str) -> Dict:
        """
        标记为正常，设置is_active=1
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    UPDATE evdata 
                    SET is_active = 1,
                        data_status = 0,
                        status_note = NULL,
                        cleaned_at = NOW(),
                        cleaned_by = :operator
                    WHERE UID = :uid
                """)
                result = conn.execute(query, {'uid': uid, 'operator': operator})
                conn.commit()
                
                # 数据变更后清除缓存
                self.clear_duplicate_cache()
                
                return {'success': True, 'affected_rows': result.rowcount}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "标记正常")
            return {'success': False, 'error': error_info['error_message']}
    
    def fix_address(self, uid: str, province: str, city: str, district: str, 
                    operator: str, note: str = None) -> Dict:
        """
        修正地址信息
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    UPDATE evdata 
                    SET `省份_中文` = :province,
                        `城市_中文` = :city,
                        `区县_中文` = :district,
                        data_status = 3,
                        status_note = :note,
                        cleaned_at = NOW(),
                        cleaned_by = :operator
                    WHERE UID = :uid
                """)
                result = conn.execute(query, {
                    'uid': uid,
                    'province': province,
                    'city': city,
                    'district': district,
                    'note': note or '地址修正',
                    'operator': operator
                })
                conn.commit()
                
                return {'success': True, 'affected_rows': result.rowcount}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "修正地址")
            return {'success': False, 'error': error_info['error_message']}
    
    def restore_deleted(self, uid: str, operator: str) -> Dict:
        """
        恢复已删除数据：设置is_active=1，data_status=0(正常)
        """
        try:
            with self.engine.connect() as conn:
                query = text("""
                    UPDATE evdata 
                    SET is_active = 1,
                        data_status = 0,
                        status_note = CONCAT(IFNULL(status_note, ''), ' | 已恢复'),
                        cleaned_at = NOW(),
                        cleaned_by = :operator
                    WHERE UID = :uid AND is_active = 0
                """)
                result = conn.execute(query, {'uid': uid, 'operator': operator})
                conn.commit()
                
                return {'success': True, 'affected_rows': result.rowcount}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "恢复数据")
            return {'success': False, 'error': error_info['error_message']}
    
    # ==================== 已删除数据管理 ====================
    
    def get_deleted_records(self, page: int = 1, page_size: int = 50) -> Dict:
        """
        获取已删除的记录列表（is_active=0）
        """
        try:
            query = text("""
                SELECT 
                    UID,
                    `充电桩编号`,
                    `运营商名称`,
                    `省份_中文`,
                    `城市_中文`,
                    `充电站位置`,
                    status_note,
                    cleaned_at,
                    cleaned_by
                FROM evdata
                WHERE is_active = 0
                ORDER BY cleaned_at DESC
                LIMIT :limit OFFSET :offset
            """)
            
            offset = (page - 1) * page_size
            df = pd.read_sql(query, self.engine, params={'limit': page_size, 'offset': offset})
            
            # 获取总数
            count_query = text("SELECT COUNT(*) as total FROM evdata WHERE is_active = 0")
            total_df = pd.read_sql(count_query, self.engine)
            total = int(total_df.iloc[0]['total']) if not total_df.empty else 0
            
            return {
                'success': True,
                'data': df.to_dict('records'),
                'total': total,
                'page': page,
                'page_size': page_size
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "获取已删除数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
    
    # ==================== 统计方法 ====================
    
    def get_cleaning_stats(self) -> Dict:
        """
        获取清洗统计
        
        data_status 状态码:
          0 = normal (正常)
          1 = duplicate (重复)
          2 = suspect (疑似)
          3 = fixed (已修复)
          4 = deleted (已删除)
        """
        try:
            query = text("""
                SELECT 
                    COALESCE(data_status, 0) as status,
                    COUNT(*) as count
                FROM evdata
                GROUP BY data_status
            """)
            df = pd.read_sql(query, self.engine)
            
            # 状态码到键名的映射
            status_map = {
                0: 'normal',
                1: 'duplicate',
                2: 'suspect',
                3: 'fixed',
                4: 'deleted'
            }
            
            stats = {
                'normal': 0,
                'duplicate': 0,
                'suspect': 0,
                'fixed': 0,
                'deleted': 0,
                'total': 0
            }
            
            for _, row in df.iterrows():
                status_code = int(row['status']) if row['status'] is not None else 0
                count = int(row['count'])
                status_key = status_map.get(status_code, 'normal')
                stats[status_key] += count
                stats['total'] += count
            
            return {'success': True, 'stats': stats}
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "获取清洗统计")
            return {'success': False, 'error': error_info['error_message'], 'stats': {}}
    
    def get_next_duplicate_group_id(self) -> int:
        """获取下一个重复组ID"""
        try:
            query = text("SELECT COALESCE(MAX(duplicate_group_id), 0) + 1 as next_id FROM evdata")
            df = pd.read_sql(query, self.engine)
            return int(df.iloc[0]['next_id']) if not df.empty else 1
        except:
            return 1
    
    # ==================== 全量扫描方法（带进度回调） ====================
    
    def get_duplicate_total(self, scan_type: str = 'full') -> int:
        """
        获取重复数据总数
        :param scan_type: 'full' 完全重复, 'suspect' 高度疑似
        """
        try:
            if scan_type == 'full':
                query = text("""
                    SELECT COUNT(*) as total FROM (
                        SELECT 1
                        FROM evdata
                        WHERE is_active = 1
                        GROUP BY 
                            `充电桩编号`, `运营商名称`, `省份_中文`, `城市_中文`,
                            `区县_中文`, `充电站位置`, `充电桩类型_转换`, `额定功率`, `所属充电站编号`
                        HAVING COUNT(*) > 1
                    ) as t
                """)
            else:
                query = text("""
                    SELECT COUNT(*) as total FROM (
                        SELECT 1
                        FROM evdata
                        WHERE is_active = 1
                        GROUP BY `充电桩编号`, `运营商名称`, `充电站位置`, `额定功率`
                        HAVING COUNT(*) > 1
                    ) as t
                """)
            
            df = pd.read_sql(query, self.engine)
            return int(df.iloc[0]['total']) if not df.empty else 0
        except:
            return 0
    
    def get_address_issue_total(self) -> int:
        """获取地址问题总数"""
        try:
            query = text("""
                SELECT COUNT(*) as total
                FROM evdata
                WHERE is_active = 1
                AND `充电站位置` IS NOT NULL
                AND `充电站位置` != ''
                AND (
                    (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                     AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                    OR
                    (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                     AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                )
            """)
            df = pd.read_sql(query, self.engine)
            return int(df.iloc[0]['total']) if not df.empty else 0
        except:
            return 0
    
    def scan_full_duplicates_all(self, batch_size: int = 100, progress_callback=None) -> Dict:
        """
        全量扫描完全重复数据 - Pandas全量模式
        32GB内存: 首次加载约60秒，重复检测约1-2秒
        
        :param batch_size: 忽略（保留参数兼容性）
        :param progress_callback: 进度回调 callback(processed, total, elapsed, message)
        """
        import time
        start_time = time.time()
        
        try:
            # 清除所有缓存，强制重新加载最新数据
            self._full_dup_cache = None
            self._data_cache = None
            if self.pandas_service:
                self.pandas_service.clear_cache()  # 关键：清除Pandas服务缓存
            
            # Pandas全量加载 + 内存检测
            self._build_full_duplicate_cache(progress_callback=progress_callback)
            
            return {
                'success': True,
                'data': self._full_dup_cache or [],
                'total': len(self._full_dup_cache) if self._full_dup_cache else 0,
                'elapsed': time.time() - start_time
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "全量扫描完全重复数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
    
    def scan_high_suspect_all(self, batch_size: int = 100, progress_callback=None) -> Dict:
        """
        全量扫描高度疑似重复数据 - Pandas全量模式
        如果数据已在内存中，扫描仅需1-2秒
        """
        import time
        start_time = time.time()
        
        try:
            # 清除所有缓存，强制重新加载最新数据
            self._high_suspect_cache = None
            self._data_cache = None
            if self.pandas_service:
                self.pandas_service.clear_cache()  # 关键：清除Pandas服务缓存
            
            # Pandas全量模式检测
            self._build_high_suspect_cache(progress_callback=progress_callback)
            
            return {
                'success': True,
                'data': self._high_suspect_cache or [],
                'total': len(self._high_suspect_cache) if self._high_suspect_cache else 0,
                'elapsed': time.time() - start_time
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "全量扫描高度疑似数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
    
    def scan_address_issues_all(self, batch_size: int = 100, progress_callback=None) -> Dict:
        """
        全量扫描地址问题数据（带进度回调）
        """
        import time
        start_time = time.time()
        
        try:
            total = self.get_address_issue_total()
            if total == 0:
                return {'success': True, 'data': [], 'total': 0}
            
            all_data = []
            offset = 0
            
            while offset < total:
                query = text("""
                    SELECT 
                        UID, `充电桩编号`, `运营商名称`,
                        `省份_中文`, `城市_中文`, `区县_中文`, `充电站位置`
                    FROM evdata
                    WHERE is_active = 1
                    AND `充电站位置` IS NOT NULL
                    AND `充电站位置` != ''
                    AND (
                        (`省份_中文` IS NOT NULL AND `省份_中文` != '' 
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(REPLACE(`省份_中文`, '省', ''), '市', ''), '%'))
                        OR
                        (`城市_中文` IS NOT NULL AND `城市_中文` != ''
                         AND `充电站位置` NOT LIKE CONCAT('%', REPLACE(`城市_中文`, '市', ''), '%'))
                    )
                    LIMIT :limit OFFSET :offset
                """)
                
                df = pd.read_sql(query, self.engine, params={'limit': batch_size, 'offset': offset})
                records = df.to_dict('records')
                
                # 添加地址解析
                for record in records:
                    parsed = self.address_parser.parse(record.get('充电站位置', ''))
                    record['parsed_province'] = parsed['province']
                    record['parsed_city'] = parsed['city']
                    record['parsed_district'] = parsed['district']
                    record['parse_confidence'] = parsed['confidence']
                
                all_data.extend(records)
                
                offset += batch_size
                elapsed = time.time() - start_time
                
                if progress_callback:
                    progress_callback(min(offset, total), total, elapsed)
            
            return {
                'success': True,
                'data': all_data,
                'total': total,
                'elapsed': time.time() - start_time
            }
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "全量扫描地址问题数据")
            return {'success': False, 'error': error_info['error_message'], 'data': [], 'total': 0}
