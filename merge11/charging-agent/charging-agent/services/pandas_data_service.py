# services/pandas_data_service.py - Pandas全量数据服务（单例模式）

import time
import threading
from typing import Dict, List, Optional, Callable
import pandas as pd
from sqlalchemy import text
from utils.db_utils import get_shared_engine


class PandasDataService:
    """
    Pandas全量数据服务（单例模式）
    
    将数据库数据全量加载到内存，提供毫秒级查询响应。
    - 首次加载约30秒（409万条数据）
    - 后续所有操作毫秒级
    - 自动管理缓存过期
    - 线程安全
    
    使用方式：
        service = PandasDataService.get_instance()
        df = service.get_dataframe()  # 获取全量数据
        preview = service.preview(10)  # 随机预览10条
        stats = service.get_statistics()  # 获取统计
    """
    
    _instance = None
    _lock = threading.Lock()
    
    # 核心字段（用于预览和分析）
    CORE_FIELDS = [
        'UID', '充电桩编号', '所属充电站编号',
        '运营商名称', '省份_中文', '城市_中文', '区县_中文',
        '充电站位置', '额定功率', '充电桩类型_转换',
        '充电站投入使用时间', '充电桩生产日期'
    ]
    
    # 预览字段（精简版，8个字段）
    PREVIEW_FIELDS = [
        '运营商名称', '省份_中文', '城市_中文', '区县_中文',
        '充电站位置', '充电桩类型_转换', '额定功率', '充电站投入使用时间'
    ]
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化（仅执行一次）"""
        if self._initialized:
            return
        
        self._df = None
        self._load_time = None
        self._ttl = 300  # 5分钟缓存
        self._engine = None
        self._table_name = 'evdata'
        self._loading = False
        self._load_lock = threading.Lock()
        self._total_count = None
        
        # 统计结果缓存（避免重复计算）
        self._stats_cache = None
        self._stats_cache_time = None
        
        self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'PandasDataService':
        """获取单例实例"""
        return cls()
    
    @property
    def engine(self):
        """获取数据库引擎"""
        if self._engine is None:
            self._engine = get_shared_engine()
        return self._engine
    
    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if self._df is None or self._load_time is None:
            return False
        return (time.time() - self._load_time) < self._ttl
    
    def get_dataframe(self, progress_callback: Optional[Callable] = None, 
                      force_reload: bool = False) -> pd.DataFrame:
        """
        获取全量数据 DataFrame（自动管理缓存）
        
        :param progress_callback: 进度回调函数 callback(current, total, message)
        :param force_reload: 是否强制重新加载
        :return: 全量数据 DataFrame
        """
        # 检查缓存
        if not force_reload and self._is_cache_valid():
            return self._df
        
        # 防止重复加载
        with self._load_lock:
            # 双重检查
            if not force_reload and self._is_cache_valid():
                return self._df
            
            self._loading = True
            try:
                self._df = self._load_data(progress_callback)
                self._load_time = time.time()
            finally:
                self._loading = False
        
        return self._df
    
    def _load_data(self, progress_callback: Optional[Callable] = None) -> pd.DataFrame:
        """
        从数据库加载全量数据
        
        :param progress_callback: 进度回调函数
        :return: DataFrame
        """
        if progress_callback:
            progress_callback(0, 100, "正在获取数据总量...")
        
        # 获取总数
        count_query = f"SELECT COUNT(*) as total FROM `{self._table_name}` WHERE is_active = 1"
        count_result = pd.read_sql(text(count_query), self.engine)
        self._total_count = int(count_result.iloc[0]['total'])
        
        if progress_callback:
            progress_callback(5, 100, f"共 {self._total_count:,} 条数据，开始加载...")
        
        # 构建查询字段
        fields_sql = ', '.join([f'`{f}`' for f in self.CORE_FIELDS])
        
        # 分批加载以显示进度
        batch_size = 500000  # 每批50万条
        all_data = []
        offset = 0
        
        while offset < self._total_count:
            if progress_callback:
                progress = min(95, int(5 + (offset / self._total_count) * 90))
                progress_callback(progress, 100, 
                    f"正在加载数据... {offset:,}/{self._total_count:,} ({offset*100//self._total_count}%)")
            
            query = f"""
                SELECT {fields_sql} 
                FROM `{self._table_name}` 
                WHERE is_active = 1 
                LIMIT {batch_size} OFFSET {offset}
            """
            batch_df = pd.read_sql(text(query), self.engine)
            
            if batch_df.empty:
                break
            
            all_data.append(batch_df)
            offset += len(batch_df)
        
        if progress_callback:
            progress_callback(98, 100, "正在合并数据...")
        
        # 合并所有批次
        if all_data:
            df = pd.concat(all_data, ignore_index=True)
        else:
            df = pd.DataFrame(columns=self.CORE_FIELDS)
        
        if progress_callback:
            progress_callback(100, 100, f"加载完成！共 {len(df):,} 条数据")
        
        return df
    
    def clear_cache(self):
        """清除缓存（数据变更后调用）"""
        with self._load_lock:
            self._df = None
            self._load_time = None
            self._total_count = None
            self._stats_cache = None
            self._stats_cache_time = None
    
    def is_loaded(self) -> bool:
        """数据是否已加载"""
        return self._is_cache_valid()
    
    def is_loading(self) -> bool:
        """是否正在加载"""
        return self._loading
    
    def get_cache_info(self) -> Dict:
        """获取缓存信息"""
        return {
            'is_loaded': self._is_cache_valid(),
            'is_loading': self._loading,
            'record_count': len(self._df) if self._df is not None else 0,
            'load_time': self._load_time,
            'cache_age': time.time() - self._load_time if self._load_time else None,
            'ttl': self._ttl,
            'memory_mb': self._df.memory_usage(deep=True).sum() / 1024 / 1024 if self._df is not None else 0
        }
    
    # ==================== 数据预览功能 ====================
    
    def preview(self, n: int = 10, use_preview_fields: bool = True) -> pd.DataFrame:
        """
        随机预览数据（毫秒级）
        
        :param n: 预览条数
        :param use_preview_fields: 是否只返回预览字段
        :return: 预览数据 DataFrame
        """
        df = self.get_dataframe()
        
        # 随机采样
        sample_n = min(n, len(df))
        result = df.sample(n=sample_n)
        
        # 返回指定字段
        if use_preview_fields:
            available_fields = [f for f in self.PREVIEW_FIELDS if f in result.columns]
            return result[available_fields]
        
        return result
    
    def preview_dict(self, n: int = 10, use_preview_fields: bool = True) -> Dict:
        """
        预览数据（返回字典格式，兼容现有接口）
        
        :param n: 预览条数
        :param use_preview_fields: 是否只返回预览字段
        :return: 预览结果字典
        """
        try:
            df = self.preview(n, use_preview_fields)
            return {
                'success': True,
                'data': df,
                'row_count': len(df),
                'column_count': len(df.columns),
                'is_core_fields': use_preview_fields,
                'actual_limit': n,
                'random_sample': True,
                'source': 'pandas_cache'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'source': 'pandas_cache'
            }
    
    # ==================== 统计分析功能 ====================
    
    def get_statistics(self, 
                       filters: Optional[Dict] = None,
                       group_limit: int = 100) -> Dict:
        """
        获取统计数据（有缓存时毫秒级，无缓存时首次计算约5秒）
        
        :param filters: 筛选条件 {
            'province': '北京市',
            'city': '北京市', 
            'district': '朝阳区',
            'operators': ['特来电', '星星充电'],
            'power_min': 7,
            'power_max': 120,
            'charge_types': ['直流'],
            'date_field': '充电站投入使用时间',
            'start_date': datetime.date,
            'end_date': datetime.date
        }
        :param group_limit: 分组统计返回数量限制
        :return: 统计结果字典
        """
        df = self.get_dataframe()
        
        # 无筛选条件时使用缓存
        if not filters:
            if self._stats_cache is not None and self._stats_cache_time:
                if (time.time() - self._stats_cache_time) < self._ttl:
                    return self._stats_cache.copy()
        
        # 应用筛选条件
        if filters:
            df = self._apply_filters(df, filters)
        
        stats = {
            'total_records': len(df),
            'columns': list(df.columns),
            'basic_stats': {},
            'source': 'pandas_cache'
        }
        
        if len(df) == 0:
            return stats
        
        # 唯一值统计
        stats['basic_stats']['unique_piles'] = int(df['充电桩编号'].nunique()) if '充电桩编号' in df.columns else 0
        stats['basic_stats']['unique_stations'] = int(df['所属充电站编号'].nunique()) if '所属充电站编号' in df.columns else 0
        
        # 按运营商统计
        if '运营商名称' in df.columns:
            stats['basic_stats']['by_operator'] = self._group_count(df, '运营商名称', group_limit)
        
        # 按区域统计（智能选择级别）
        stats['basic_stats'].update(self._get_region_stats(df, filters, group_limit))
        
        # 按类型统计
        if '充电桩类型_转换' in df.columns:
            stats['basic_stats']['by_type'] = self._group_count(df, '充电桩类型_转换', group_limit)
        
        # 按充电站统计
        if '所属充电站编号' in df.columns:
            stats['basic_stats']['by_station'] = self._group_count(df, '所属充电站编号', group_limit)
        
        # 功率统计
        if '额定功率' in df.columns:
            stats['basic_stats']['power_stats'] = self._get_power_stats(df)
        
        # 无筛选条件时缓存结果
        if not filters:
            self._stats_cache = stats.copy()
            self._stats_cache_time = time.time()
        
        return stats
    
    def _apply_filters(self, df: pd.DataFrame, filters: Dict) -> pd.DataFrame:
        """应用筛选条件"""
        result = df.copy()
        
        # 省份筛选
        if filters.get('province') and '省份_中文' in result.columns:
            result = result[result['省份_中文'] == filters['province']]
        
        # 城市筛选
        if filters.get('city') and '城市_中文' in result.columns:
            result = result[result['城市_中文'] == filters['city']]
        
        # 区县筛选
        if filters.get('district') and '区县_中文' in result.columns:
            result = result[result['区县_中文'] == filters['district']]
        
        # 运营商筛选
        if filters.get('operators') and '运营商名称' in result.columns:
            result = result[result['运营商名称'].isin(filters['operators'])]
        
        # 功率范围筛选
        if '额定功率' in result.columns:
            if filters.get('power_min') is not None:
                result = result[result['额定功率'] >= filters['power_min']]
            if filters.get('power_max') is not None:
                result = result[result['额定功率'] <= filters['power_max']]
            # 功率比较：大于/小于/等于/大于等于/小于等于 + 数值；或 介于 + 最小值～最大值
            if filters.get('power_op') == '介于' and filters.get('power_value_min') is not None and filters.get('power_value_max') is not None:
                lo, hi = float(filters['power_value_min']), float(filters['power_value_max'])
                if lo > hi:
                    lo, hi = hi, lo
                col = pd.to_numeric(result['额定功率'], errors='coerce')
                result = result[(col >= lo) & (col <= hi)]
            elif filters.get('power_op') in ('大于', '小于', '等于', '大于等于', '小于等于') and filters.get('power_value') is not None:
                pv = float(filters['power_value'])
                col = pd.to_numeric(result['额定功率'], errors='coerce')
                if filters['power_op'] == '大于':
                    result = result[col > pv]
                elif filters['power_op'] == '小于':
                    result = result[col < pv]
                elif filters['power_op'] == '等于':
                    result = result[col == pv]
                elif filters['power_op'] == '大于等于':
                    result = result[col >= pv]
                elif filters['power_op'] == '小于等于':
                    result = result[col <= pv]
        
        # 充电类型筛选
        if filters.get('charge_types') and '充电桩类型_转换' in result.columns:
            result = result[result['充电桩类型_转换'].isin(filters['charge_types'])]
        
        # 日期筛选
        date_field = filters.get('date_field')
        if date_field and date_field in result.columns:
            if filters.get('start_date'):
                result = result[pd.to_datetime(result[date_field]) >= pd.to_datetime(filters['start_date'])]
            if filters.get('end_date'):
                result = result[pd.to_datetime(result[date_field]) <= pd.to_datetime(filters['end_date'])]
        
        return result
    
    def _group_count(self, df: pd.DataFrame, field: str, limit: int) -> Dict:
        """按字段分组统计"""
        if field not in df.columns:
            return {}
        
        counts = df.groupby(field).size().sort_values(ascending=False).head(limit)
        return {str(k): int(v) for k, v in counts.items()}
    
    def _get_region_stats(self, df: pd.DataFrame, filters: Optional[Dict], group_limit: int) -> Dict:
        """获取区域统计（智能选择级别）"""
        result = {}
        
        direct_cities = ['北京市', '上海市', '天津市', '重庆市', '北京', '上海', '天津', '重庆']
        
        # 根据筛选条件智能选择统计级别
        if filters:
            province = filters.get('province', '')
            city = filters.get('city', '')
            district = filters.get('district', '')
            
            if province in direct_cities or city in direct_cities:
                # 直辖市：按区统计
                result['by_location'] = self._group_count(df, '区县_中文', group_limit)
            elif district:
                # 已选区县：按区统计
                result['by_location'] = self._group_count(df, '区县_中文', group_limit)
            elif city:
                # 已选城市：按区统计
                result['by_location'] = self._group_count(df, '区县_中文', group_limit)
            elif province:
                # 已选省份：按市统计
                result['by_city'] = self._group_count(df, '城市_中文', group_limit)
                result['by_location'] = self._group_count(df, '区县_中文', group_limit)
            else:
                # 无筛选：按省统计
                result['by_province'] = self._group_count(df, '省份_中文', group_limit)
                result['by_city'] = self._group_count(df, '城市_中文', group_limit)
                result['by_location'] = self._group_count(df, '区县_中文', group_limit)
        else:
            # 无筛选：全部级别
            result['by_province'] = self._group_count(df, '省份_中文', group_limit)
            result['by_city'] = self._group_count(df, '城市_中文', group_limit)
            result['by_location'] = self._group_count(df, '区县_中文', group_limit)
        
        return result
    
    def _get_power_stats(self, df: pd.DataFrame) -> Dict:
        """获取功率统计"""
        power_col = df['额定功率']
        valid_power = power_col[(power_col.notna()) & (power_col > 0)]
        
        if len(valid_power) == 0:
            return {}
        
        return {
            'total_count': len(valid_power),
            'mean': float(valid_power.mean()),
            'min': float(valid_power.min()),
            'max': float(valid_power.max()),
            'std': float(valid_power.std()),
            'by_range': {
                '≤7kW（慢充）': int((valid_power <= 7).sum()),
                '7-30kW（小功率）': int(((valid_power > 7) & (valid_power <= 30)).sum()),
                '30-60kW（中功率）': int(((valid_power > 30) & (valid_power <= 60)).sum()),
                '60-120kW（大功率）': int(((valid_power > 60) & (valid_power <= 120)).sum()),
                '>120kW（超快充）': int((valid_power > 120).sum())
            }
        }
    
    # ==================== 区域字典功能 ====================
    
    def get_provinces(self) -> List[str]:
        """获取所有省份列表"""
        df = self.get_dataframe()
        if '省份_中文' not in df.columns:
            return []
        return sorted(df['省份_中文'].dropna().unique().tolist())
    
    def get_cities(self, province: Optional[str] = None) -> List[str]:
        """获取城市列表"""
        df = self.get_dataframe()
        if '城市_中文' not in df.columns:
            return []
        
        if province and '省份_中文' in df.columns:
            df = df[df['省份_中文'] == province]
        
        return sorted(df['城市_中文'].dropna().unique().tolist())
    
    def get_districts(self, province: Optional[str] = None, city: Optional[str] = None) -> List[str]:
        """获取区县列表"""
        df = self.get_dataframe()
        if '区县_中文' not in df.columns:
            return []
        
        if province and '省份_中文' in df.columns:
            df = df[df['省份_中文'] == province]
        if city and '城市_中文' in df.columns:
            df = df[df['城市_中文'] == city]
        
        return sorted(df['区县_中文'].dropna().unique().tolist())
    
    def get_operators(self) -> List[str]:
        """获取所有运营商列表"""
        df = self.get_dataframe()
        if '运营商名称' not in df.columns:
            return []
        return sorted(df['运营商名称'].dropna().unique().tolist())
    
    # ==================== 数据清洗支持 ====================
    
    def get_dataframe_for_cleaning(self) -> pd.DataFrame:
        """获取用于数据清洗的 DataFrame（包含UID等字段）"""
        return self.get_dataframe()
