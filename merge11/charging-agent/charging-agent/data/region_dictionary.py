# -*- coding: utf-8 -*-
# data/region_dictionary.py - 区域数据字典类

import pandas as pd
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from utils.db_utils import create_db_engine


class RegionDictionary:
    """
    区域数据字典类
    负责从数据库加载区域数据（省份、城市、区县），并提供三级联动查询功能
    支持数据缓存，避免重复查询数据库
    """
    
    def __init__(self, table_name: str = 'evdata'):
        """
        初始化区域数据字典
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self._data: Optional[Dict] = None
        self._last_update: Optional[str] = None
        
        # 创建数据库连接（使用统一工具函数）
        self.engine = create_db_engine(echo=False)
    
    def load_from_database(self, force_refresh: bool = False) -> Dict:
        """
        从数据库加载区域数据
        :param force_refresh: 是否强制刷新（忽略缓存）
        :return: 区域数据字典
        """
        # 如果已有缓存且不强制刷新，直接返回
        if self._data and not force_refresh:
            return self._data
        
        try:
            # 查询数据库，获取所有唯一的省份、城市、区县组合（排除已删除数据）
            query = text(f"""
                SELECT DISTINCT 
                    `省份_中文`,
                    `城市_中文`,
                    `区县_中文`
                FROM `{self.table_name}`
                WHERE `省份_中文` IS NOT NULL
                  AND `省份_中文` != ''
                  AND is_active = 1
                ORDER BY `省份_中文`, `城市_中文`, `区县_中文`
            """)
            
            df = pd.read_sql(query, self.engine)

            # 兼容历史脏数据：部分记录把“省份前缀”拼进了城市/区县中文名（例如“贵州省六盘水市”）
            # 这里仅用于下拉列表展示去重，不会修改数据库中的原始数据。
            if '城市_中文' in df.columns:
                city = df['城市_中文'].astype(str).str.strip()
                mask_prefixed_city = (
                    city.str.contains(r'(省|自治区|特别行政区)', regex=True) &
                    city.str.contains(r'(市|地区|盟|自治州)$', regex=True)
                )
                if mask_prefixed_city.any():
                    df.loc[mask_prefixed_city, '城市_中文'] = city.loc[mask_prefixed_city].str.replace(
                        r'^.*?(?:省|自治区|特别行政区)', '', regex=True
                    )
            
            # 构建三级联动数据结构
            provinces = sorted(df['省份_中文'].dropna().unique().tolist())
            
            # 构建省份-城市映射
            province_cities: Dict[str, List[str]] = {}
            for province in provinces:
                cities = df[df['省份_中文'] == province]['城市_中文'].dropna().unique().tolist()
                province_cities[province] = sorted(cities) if cities else []
            
            # 构建省份-城市-区县映射（排除区县字段中的明显脏数据）
            province_city_districts: Dict[Tuple[str, str], List[str]] = {}
            provinces_set = set(provinces)
            for _, row in df.iterrows():
                province = row['省份_中文']
                city = row['城市_中文']
                district = row['区县_中文']

                if pd.notna(province) and pd.notna(city) and pd.notna(district):
                    d = str(district).strip()
                    c = str(city).strip() if pd.notna(city) else ''
                    # 排除：空、"全部"、省份名、以 省/自治区/特别行政区 结尾、与城市同名（常为“市直”误入）
                    if not d or d == '全部':
                        continue
                    if d in provinces_set:
                        continue
                    if d.endswith('省') or d.endswith('自治区') or d.endswith('特别行政区'):
                        continue
                    if c and d == c:
                        continue

                    key = (province, city)
                    if key not in province_city_districts:
                        province_city_districts[key] = []
                    if d not in province_city_districts[key]:
                        province_city_districts[key].append(d)

            # 对区县列表排序
            for key in province_city_districts:
                province_city_districts[key] = sorted(province_city_districts[key])
            
            # 构建返回数据
            self._data = {
                'provinces': provinces,
                'province_cities': province_cities,
                'province_city_districts': province_city_districts,
                'total_provinces': len(provinces),
                'total_cities': sum(len(cities) for cities in province_cities.values()),
                'total_districts': sum(len(districts) for districts in province_city_districts.values())
            }
            
            from datetime import datetime
            self._last_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return self._data
            
        except Exception as e:
            print(f"⚠️  加载区域数据失败: {str(e)}")
            # 返回空数据
            return {
                'provinces': [],
                'province_cities': {},
                'province_city_districts': {},
                'total_provinces': 0,
                'total_cities': 0,
                'total_districts': 0
            }
    
    def get_provinces(self) -> List[str]:
        """
        获取省份列表
        :return: 省份列表
        """
        if not self._data:
            self.load_from_database()
        return self._data.get('provinces', [])
    
    def get_cities(self, province: str) -> List[str]:
        """
        根据省份获取城市列表
        :param province: 省份名称
        :return: 城市列表
        """
        if not self._data:
            self.load_from_database()
        
        if province and province in self._data.get('province_cities', {}):
            return self._data['province_cities'][province]
        return []
    
    def get_districts(self, province: str, city: str) -> List[str]:
        """
        根据省份和城市获取区县列表
        :param province: 省份名称
        :param city: 城市名称
        :return: 区县列表
        """
        if not self._data:
            self.load_from_database()
        
        key = (province, city)
        if key in self._data.get('province_city_districts', {}):
            return self._data['province_city_districts'][key]
        return []
    
    def is_cached(self) -> bool:
        """
        检查数据是否已缓存
        :return: 是否已缓存
        """
        return self._data is not None
    
    def get_last_update(self) -> Optional[str]:
        """
        获取最后更新时间
        :return: 最后更新时间字符串
        """
        return self._last_update
    
    def clear_cache(self):
        """
        清除缓存，强制下次重新加载
        """
        self._data = None
        self._last_update = None
    
    def get_statistics(self) -> Dict:
        """
        获取区域数据统计信息
        :return: 统计信息字典
        """
        if not self._data:
            self.load_from_database()
        
        return {
            'total_provinces': self._data.get('total_provinces', 0),
            'total_cities': self._data.get('total_cities', 0),
            'total_districts': self._data.get('total_districts', 0),
            'last_update': self._last_update
        }

