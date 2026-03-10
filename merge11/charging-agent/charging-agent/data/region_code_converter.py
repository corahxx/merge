# data/region_code_converter.py - 区域代码转换器
"""
根据行政区划代码将数字代码转换为正确的中文名称
用于数据导入时的校验和修正
"""

import json
import os
from typing import Dict, Optional, Tuple


class RegionCodeConverter:
    """区域代码转换器 - 单例模式"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if RegionCodeConverter._initialized:
            return
        
        self.provinces: Dict[str, str] = {}
        self.cities: Dict[str, str] = {}
        self.districts: Dict[str, str] = {}
        self._load_mapping()
        RegionCodeConverter._initialized = True
    
    def _load_mapping(self):
        """加载区域代码映射文件"""
        # 获取映射文件路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_file = os.path.join(current_dir, 'region_code_mapping.json')
        
        if not os.path.exists(mapping_file):
            print(f"警告: 区域代码映射文件不存在: {mapping_file}")
            return
        
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.provinces = data.get('省份映射', {})
            self.cities = data.get('城市映射', {})
            self.districts = data.get('区县映射', {})
            
            print(f"✅ 已加载区域代码映射: {len(self.provinces)} 省份, {len(self.cities)} 城市, {len(self.districts)} 区县")
        except Exception as e:
            print(f"❌ 加载区域代码映射失败: {e}")
    
    def get_province_name(self, code: str) -> Optional[str]:
        """根据省份代码获取省份名称"""
        if not code:
            return None
        code_str = str(code).strip()
        # 省份代码可能是 110000 或 11
        if len(code_str) == 2:
            code_str = code_str + "0000"
        elif len(code_str) == 6 and not code_str.endswith("0000"):
            # 从区县代码提取省份代码
            code_str = code_str[:2] + "0000"
        return self.provinces.get(code_str)
    
    def get_city_name(self, code: str) -> Optional[str]:
        """根据城市代码获取城市名称"""
        if not code:
            return None
        code_str = str(code).strip()
        # 城市代码可能是 1101 或 110100
        if len(code_str) == 4:
            code_str = code_str + "00"
        elif len(code_str) == 6 and not code_str.endswith("00"):
            # 从区县代码提取城市代码
            code_str = code_str[:4] + "00"
        return self.cities.get(code_str)
    
    def get_district_name(self, code: str) -> Optional[str]:
        """根据区县代码获取区县名称"""
        if not code:
            return None
        code_str = str(code).strip()
        # 确保是6位代码
        if len(code_str) < 6:
            code_str = code_str.zfill(6)
        return self.districts.get(code_str)
    
    def convert_province(self, code: str, current_name: str = None) -> Tuple[str, bool]:
        """
        转换省份：根据代码获取正确的省份名称
        返回: (正确的名称, 是否被修正)
        """
        correct_name = self.get_province_name(code)
        if correct_name:
            if current_name and current_name != correct_name:
                return correct_name, True
            return correct_name, False
        # 如果没有找到映射，返回原值
        return current_name or '', False
    
    def convert_city(self, code: str, current_name: str = None) -> Tuple[str, bool]:
        """
        转换城市：根据代码获取正确的城市名称
        返回: (正确的名称, 是否被修正)
        """
        correct_name = self.get_city_name(code)
        if correct_name:
            if current_name and current_name != correct_name:
                return correct_name, True
            return correct_name, False
        return current_name or '', False
    
    def convert_district(self, code: str, current_name: str = None) -> Tuple[str, bool]:
        """
        转换区县：根据代码获取正确的区县名称
        返回: (正确的名称, 是否被修正)
        """
        correct_name = self.get_district_name(code)
        if correct_name:
            if current_name and current_name != correct_name:
                return correct_name, True
            return correct_name, False
        return current_name or '', False
    
    def validate_and_fix_region(self, province_code: str, province_name: str,
                                 city_code: str, city_name: str,
                                 district_code: str, district_name: str) -> Dict:
        """
        校验并修正省市区信息
        返回修正后的结果和修正统计
        """
        result = {
            'province': province_name,
            'city': city_name,
            'district': district_name,
            'province_fixed': False,
            'city_fixed': False,
            'district_fixed': False
        }
        
        # 修正省份
        if province_code:
            correct_province, fixed = self.convert_province(province_code, province_name)
            if correct_province:
                result['province'] = correct_province
                result['province_fixed'] = fixed
        
        # 修正城市
        if city_code:
            correct_city, fixed = self.convert_city(city_code, city_name)
            if correct_city:
                result['city'] = correct_city
                result['city_fixed'] = fixed
        
        # 修正区县
        if district_code:
            correct_district, fixed = self.convert_district(district_code, district_name)
            if correct_district:
                result['district'] = correct_district
                result['district_fixed'] = fixed
        
        return result


# 全局单例实例
_converter_instance = None

def get_converter() -> RegionCodeConverter:
    """获取区域代码转换器实例"""
    global _converter_instance
    if _converter_instance is None:
        _converter_instance = RegionCodeConverter()
    return _converter_instance
