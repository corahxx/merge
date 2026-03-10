# -*- coding: utf-8 -*-
# data/strict_data_cleaner.py - 严格数据清洗器
# 基于数据清洗重构可行性分析文档实现
# 目标：数据准确率≥98%，批次成功率<95%暂停导入

import pandas as pd
import numpy as np
import re
import uuid
import hashlib
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from sqlalchemy import text
from core.knowledge_base import KnowledgeBase


class StrictDataCleaner:
    """
    严格数据清洗器
    
    功能特点：
    1. 严格数据验证：每个字段有明确的验证规则
    2. 智能数据修复：自动修复可修复的问题
    3. 异常数据记录：所有异常数据记录到日志
    4. 数据血缘追溯：每条记录可追溯来源文件和行号
    5. 批量处理优化：支持大文件高效处理
    """
    
    # 必填字段
    REQUIRED_FIELDS = ['运营商名称', '省份_中文', '城市_中文', '充电桩类型_转换']
    
    # 直辖市列表
    DIRECT_CITIES = ['北京市', '上海市', '天津市', '重庆市']
    
    # 功率有效范围
    POWER_MIN = 0
    POWER_MAX = 1000
    
    # 经纬度范围（中国境内）
    LONGITUDE_MIN = 73.66
    LONGITUDE_MAX = 135.05
    LATITUDE_MIN = 3.86
    LATITUDE_MAX = 53.55
    
    # 质量评分阈值
    QUALITY_THRESHOLD_REJECT = 60  # 低于此分数拒绝导入
    QUALITY_THRESHOLD_WARNING = 80  # 低于此分数标记警告
    
    # 批次成功率阈值
    SUCCESS_RATE_THRESHOLD = 0.95  # 95%
    
    def __init__(self, engine=None, batch_id: str = None, source_file: str = None,
                 verbose: bool = True, batch_size: int = 500):
        """
        初始化严格数据清洗器
        
        :param engine: SQLAlchemy数据库引擎
        :param batch_id: 导入批次ID（自动生成或指定）
        :param source_file: 来源文件名
        :param verbose: 是否显示详细日志
        :param batch_size: 批次大小（默认500条）
        """
        self.engine = engine
        self.batch_id = batch_id or self._generate_batch_id()
        self.source_file = source_file or ''
        self.verbose = verbose
        self.batch_size = batch_size
        
        # 设置日志
        self._setup_logging()
        
        # 统计信息
        self.stats = {
            'total_rows': 0,
            'success_rows': 0,
            'failed_rows': 0,
            'warning_rows': 0,
            'fixed_rows': 0,
        }
        
        # 错误日志列表
        self.error_logs: List[Dict] = []
        
        # 充电站ID缓存（用于检测重复和一致性）
        self.station_cache: Dict[str, Dict] = {}
        
        # 区域字典缓存
        self._region_dict: Optional[Dict] = None
        
        # 加载标准运营商集合
        self.standard_operators = KnowledgeBase.STANDARD_OPERATORS
        self.operator_aliases = KnowledgeBase.OPERATOR_NICKNAMES
    
    def _generate_batch_id(self) -> str:
        """生成导入批次ID"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"batch_{timestamp}_{uuid.uuid4().hex[:8]}"
    
    def _setup_logging(self):
        """设置日志"""
        log_dir = 'logs'
        import os
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = f"{log_dir}/import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        self.logger = logging.getLogger(f'StrictDataCleaner_{self.batch_id}')
        self.logger.setLevel(logging.DEBUG)
        
        # 文件处理器
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        
        # 控制台处理器
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO if self.verbose else logging.WARNING)
        
        # 格式
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        
        self.logger.info(f"=== 数据清洗任务开始 ===")
        self.logger.info(f"批次ID: {self.batch_id}")
        self.logger.info(f"来源文件: {self.source_file}")
    
    def log_error(self, row_num: int, field_name: str, error_type: str, 
                  error_message: str, original_value: Any = None,
                  error_level: str = 'ERROR', action: str = 'SKIPPED',
                  fixed_value: Any = None, row_data: Dict = None):
        """
        记录错误日志
        
        :param row_num: 行号
        :param field_name: 字段名
        :param error_type: 错误类型
        :param error_message: 错误描述
        :param original_value: 原始值
        :param error_level: 错误等级 (CRITICAL/ERROR/WARNING/INFO)
        :param action: 处理方式 (SKIPPED/FIXED/SET_NULL)
        :param fixed_value: 修复后的值
        :param row_data: 行数据（用于定位）
        """
        error_log = {
            'import_batch_id': self.batch_id,
            'source_file': self.source_file,
            'source_row_num': row_num,
            'field_name': field_name,
            'original_value': str(original_value)[:500] if original_value else None,
            'error_type': error_type,
            'error_level': error_level,
            'error_message': error_message[:500],
            'action_taken': action,
            'fixed_value': str(fixed_value)[:500] if fixed_value else None,
            'operator_name': row_data.get('运营商名称', '') if row_data else '',
            'province': row_data.get('省份_中文', '') if row_data else '',
            'created_at': datetime.now()
        }
        
        self.error_logs.append(error_log)
        
        # 写日志文件
        log_msg = f"[{error_level}] 行{row_num} - {field_name}: {error_message}"
        if error_level == 'CRITICAL':
            self.logger.critical(log_msg)
        elif error_level == 'ERROR':
            self.logger.error(log_msg)
        elif error_level == 'WARNING':
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
    
    # ========== 字段级校验方法 ==========
    
    def validate_operator(self, row: Dict, row_num: int) -> str:
        """
        严格校验运营商名称
        
        :param row: 行数据
        :param row_num: 行号
        :return: 标准化后的运营商名称
        """
        operator = str(row.get('运营商名称', '')).strip()
        
        if not operator or operator == 'nan':
            self.log_error(row_num, '运营商名称', 'REQUIRED_FIELD_MISSING',
                          '运营商名称为空', original_value=operator,
                          action='FIXED', fixed_value='无登记运营商', row_data=row)
            return '无登记运营商'
        
        # 1. 直接匹配标准名称
        if operator in self.standard_operators:
            return operator
        
        # 2. 别名匹配
        if operator in self.operator_aliases:
            standard_name = self.operator_aliases[operator]
            self.log_error(row_num, '运营商名称', 'OPERATOR_ALIAS_CONVERTED',
                          f'运营商别名转换: {operator} → {standard_name}',
                          original_value=operator, error_level='INFO',
                          action='FIXED', fixed_value=standard_name, row_data=row)
            return standard_name
        
        # 3. 模糊匹配（包含关系）
        for std_operator in self.standard_operators:
            if std_operator in operator or operator in std_operator:
                self.log_error(row_num, '运营商名称', 'OPERATOR_FUZZY_MATCHED',
                              f'运营商模糊匹配: {operator} → {std_operator}',
                              original_value=operator, error_level='INFO',
                              action='FIXED', fixed_value=std_operator, row_data=row)
                return std_operator
        
        # 4. 无法匹配，记录警告但保留原值
        self.log_error(row_num, '运营商名称', 'OPERATOR_NOT_IN_DICTIONARY',
                      f'未知运营商名称(不在标准字典中): {operator}',
                      original_value=operator, error_level='WARNING',
                      action='KEPT', row_data=row)
        return operator
    
    def validate_region_hierarchy(self, row: Dict, row_num: int) -> Tuple[str, str, str]:
        """
        校验省份-城市-区县三级隶属关系
        
        :param row: 行数据
        :param row_num: 行号
        :return: (province, city, district)
        """
        province = str(row.get('省份_中文', '')).strip()
        city = str(row.get('城市_中文', '')).strip()
        district = str(row.get('区县_中文', '')).strip()
        address = str(row.get('充电站位置', '')).strip()
        
        # 处理nan值
        if province == 'nan':
            province = ''
        if city == 'nan':
            city = ''
        if district == 'nan':
            district = ''
        
        # 1. 校验省份是否有效
        if not province:
            extracted = self._extract_province_from_address(address)
            if extracted:
                self.log_error(row_num, '省份_中文', 'PROVINCE_EMPTY_EXTRACTED',
                              f'省份为空，从地址提取: {extracted}',
                              original_value='', error_level='INFO',
                              action='FIXED', fixed_value=extracted, row_data=row)
                province = extracted
            else:
                self.log_error(row_num, '省份_中文', 'REQUIRED_FIELD_MISSING',
                              '省份为空且无法从地址提取',
                              original_value='', error_level='ERROR', row_data=row)
        
        # 2. 直辖市特殊处理
        if province in self.DIRECT_CITIES:
            # 直辖市的城市应该等于省份
            if city and city != province:
                self.log_error(row_num, '城市_中文', 'DIRECT_CITY_MISMATCH',
                              f'直辖市{province}的城市应为{province}，当前为{city}',
                              original_value=city, error_level='INFO',
                              action='FIXED', fixed_value=province, row_data=row)
            city = province
            
            # 区县不应等于省份/城市名
            if district == province or district == city or not district:
                extracted = self._extract_district_from_address(address, province)
                if extracted and extracted != province:
                    self.log_error(row_num, '区县_中文', 'DIRECT_CITY_DISTRICT_FIXED',
                                  f'直辖市区县修正: {district} → {extracted}',
                                  original_value=district, error_level='INFO',
                                  action='FIXED', fixed_value=extracted, row_data=row)
                    district = extracted
                elif district == province or district == city:
                    self.log_error(row_num, '区县_中文', 'DIRECT_CITY_DISTRICT_INVALID',
                                  f'直辖市{province}的区县不应为{district}，无法从地址提取',
                                  original_value=district, error_level='WARNING', row_data=row)
        
        # 3. 非直辖市：校验城市是否为空
        else:
            if not city:
                extracted = self._extract_city_from_address(address, province)
                if extracted:
                    self.log_error(row_num, '城市_中文', 'CITY_EMPTY_EXTRACTED',
                                  f'城市为空，从地址提取: {extracted}',
                                  original_value='', error_level='INFO',
                                  action='FIXED', fixed_value=extracted, row_data=row)
                    city = extracted
                else:
                    self.log_error(row_num, '城市_中文', 'REQUIRED_FIELD_MISSING',
                                  '城市为空且无法从地址提取',
                                  original_value='', error_level='ERROR', row_data=row)
        
        return province, city, district
    
    def validate_station_id(self, row: Dict, row_num: int) -> Tuple[str, bool]:
        """
        充电站ID多重校验
        
        :param row: 行数据
        :param row_num: 行号
        :return: (station_id, is_original) - ID和是否为原始ID
        """
        station_id = str(row.get('所属充电站编号', '')).strip()
        operator = str(row.get('运营商名称', '')).strip()
        address = str(row.get('充电站位置', '')).strip()
        station_name = str(row.get('充电站名称', '')).strip()
        
        # 处理nan值
        if station_id == 'nan':
            station_id = ''
        
        # 1. 格式校验：为空或太短
        if not station_id or len(station_id) < 3:
            temp_id = self._generate_temp_station_id(operator, address, station_name)
            self.log_error(row_num, '所属充电站编号', 'STATION_ID_EMPTY_GENERATED',
                          f'充电站编号为空或无效，已生成临时ID: {temp_id}',
                          original_value=station_id, error_level='INFO',
                          action='FIXED', fixed_value=temp_id, row_data=row)
            return temp_id, False
        
        # 2. 重复检测 + 一致性校验
        if station_id in self.station_cache:
            existing = self.station_cache[station_id]
            
            # 检查运营商是否一致
            if existing['operator'] != operator and existing['operator'] and operator:
                self.log_error(row_num, '所属充电站编号', 'STATION_ID_OPERATOR_MISMATCH',
                              f'站点ID {station_id} 运营商不一致: 已有={existing["operator"]}, 当前={operator}',
                              original_value=station_id, error_level='WARNING', row_data=row)
            
            # 检查地址相似度
            if existing['address'] and address:
                similarity = self._calculate_address_similarity(existing['address'], address)
                if similarity < 0.3:  # 相似度低于30%
                    new_id = self._generate_unique_station_id(station_id, operator, address)
                    self.log_error(row_num, '所属充电站编号', 'STATION_ID_ADDRESS_MISMATCH',
                                  f'站点ID {station_id} 地址差异过大(相似度{similarity:.0%})，生成新ID: {new_id}',
                                  original_value=station_id, error_level='WARNING',
                                  action='FIXED', fixed_value=new_id, row_data=row)
                    return new_id, False
        else:
            # 新站点，记录到缓存
            self.station_cache[station_id] = {
                'operator': operator,
                'address': address,
                'station_name': station_name
            }
        
        return station_id, True
    
    def validate_power(self, row: Dict, row_num: int) -> Optional[float]:
        """
        校验额定功率
        
        :param row: 行数据
        :param row_num: 行号
        :return: 有效的功率值或None
        """
        power = row.get('额定功率')
        
        # 空值处理
        if power is None or (isinstance(power, float) and pd.isna(power)):
            return None
        
        if isinstance(power, str):
            power = power.strip()
            if power == '' or power == 'nan':
                return None
            try:
                power = float(power)
            except ValueError:
                self.log_error(row_num, '额定功率', 'POWER_INVALID_FORMAT',
                              f'功率格式无效: {power}',
                              original_value=power, error_level='WARNING',
                              action='SET_NULL', row_data=row)
                return None
        
        # 范围校验
        if power <= self.POWER_MIN or power > self.POWER_MAX:
            self.log_error(row_num, '额定功率', 'POWER_OUT_OF_RANGE',
                          f'功率超出有效范围[{self.POWER_MIN}, {self.POWER_MAX}]: {power}',
                          original_value=power, error_level='WARNING',
                          action='SET_NULL', row_data=row)
            return None
        
        return round(power, 2)
    
    def validate_coordinates(self, row: Dict, row_num: int) -> Tuple[Optional[float], Optional[float]]:
        """
        验证和修复经纬度坐标
        
        注意：当前源数据暂无经纬度，为空时跳过校验
        
        :param row: 行数据
        :param row_num: 行号
        :return: (longitude, latitude) 或 (None, None)
        """
        lng = row.get('经度')
        lat = row.get('纬度')
        
        # 判断是否为空
        lng_empty = lng is None or (isinstance(lng, float) and pd.isna(lng)) or str(lng).strip() in ('', 'nan')
        lat_empty = lat is None or (isinstance(lat, float) and pd.isna(lat)) or str(lat).strip() in ('', 'nan')
        
        # 都为空时直接返回，不记录错误（源数据暂无经纬度）
        if lng_empty and lat_empty:
            return None, None
        
        # 只有一个为空，记录不完整
        if lng_empty != lat_empty:
            self.log_error(row_num, '经度/纬度', 'COORDINATE_INCOMPLETE',
                          f'经纬度不完整: 经度={lng}, 纬度={lat}',
                          original_value=f'{lng},{lat}', error_level='WARNING',
                          action='SET_NULL', row_data=row)
            return None, None
        
        # 格式转换
        try:
            lng = float(lng)
            lat = float(lat)
        except (ValueError, TypeError):
            self.log_error(row_num, '经度/纬度', 'COORDINATE_INVALID_FORMAT',
                          f'经纬度格式无效: 经度={lng}, 纬度={lat}',
                          original_value=f'{lng},{lat}', error_level='WARNING',
                          action='SET_NULL', row_data=row)
            return None, None
        
        # 范围验证
        if not (self.LONGITUDE_MIN <= lng <= self.LONGITUDE_MAX):
            self.log_error(row_num, '经度', 'LONGITUDE_OUT_OF_RANGE',
                          f'经度超出中国范围[{self.LONGITUDE_MIN}, {self.LONGITUDE_MAX}]: {lng}',
                          original_value=lng, error_level='WARNING',
                          action='SET_NULL', row_data=row)
            return None, None
        
        if not (self.LATITUDE_MIN <= lat <= self.LATITUDE_MAX):
            self.log_error(row_num, '纬度', 'LATITUDE_OUT_OF_RANGE',
                          f'纬度超出中国范围[{self.LATITUDE_MIN}, {self.LATITUDE_MAX}]: {lat}',
                          original_value=lat, error_level='WARNING',
                          action='SET_NULL', row_data=row)
            return None, None
        
        return round(lng, 6), round(lat, 6)
    
    # ========== 工具方法 ==========
    
    def _generate_temp_station_id(self, operator: str, address: str, name: str) -> str:
        """
        基于运营商、地址、站名生成临时站点ID
        格式: TEMP_{运营商缩写}_{哈希前8位}
        """
        content = f"{operator}_{address}_{name}"
        hash_val = hashlib.md5(content.encode()).hexdigest()[:8].upper()
        operator_abbr = operator[:2] if operator and operator != 'nan' else 'XX'
        return f"TEMP_{operator_abbr}_{hash_val}"
    
    def _generate_unique_station_id(self, original_id: str, operator: str, address: str) -> str:
        """
        生成唯一站点ID（用于ID冲突时）
        """
        content = f"{original_id}_{operator}_{address}"
        hash_val = hashlib.md5(content.encode()).hexdigest()[:6].upper()
        return f"{original_id}_{hash_val}"
    
    def _calculate_address_similarity(self, addr1: str, addr2: str) -> float:
        """
        计算两个地址的相似度（基于关键词重叠）
        """
        if not addr1 or not addr2:
            return 0.0
        
        # 提取地址关键词
        keywords1 = set(re.findall(r'[\u4e00-\u9fa5]{2,}', addr1))
        keywords2 = set(re.findall(r'[\u4e00-\u9fa5]{2,}', addr2))
        
        if not keywords1 or not keywords2:
            return 0.0
        
        intersection = keywords1 & keywords2
        union = keywords1 | keywords2
        return len(intersection) / len(union)
    
    def _extract_province_from_address(self, address: str) -> Optional[str]:
        """从地址中提取省份"""
        if not address:
            return None
        
        provinces = [
            '北京市', '上海市', '天津市', '重庆市',
            '广东省', '江苏省', '浙江省', '山东省', '河南省', '四川省',
            '湖北省', '湖南省', '河北省', '福建省', '安徽省', '辽宁省',
            '陕西省', '江西省', '广西壮族自治区', '云南省', '贵州省',
            '山西省', '内蒙古自治区', '新疆维吾尔自治区', '甘肃省',
            '吉林省', '黑龙江省', '海南省', '宁夏回族自治区', '青海省',
            '西藏自治区'
        ]
        
        for prov in provinces:
            if prov in address:
                return prov
            # 尝试去除"省"/"市"后匹配
            prov_short = prov.replace('省', '').replace('市', '').replace('自治区', '')
            if prov_short in address:
                return prov
        
        return None
    
    def _extract_city_from_address(self, address: str, province: str = None) -> Optional[str]:
        """从地址中提取城市"""
        if not address:
            return None
        
        # 匹配"xxx市"的模式
        match = re.search(r'([\u4e00-\u9fa5]{2,}市)', address)
        if match:
            city = match.group(1)
            # 排除直辖市本身
            if city not in self.DIRECT_CITIES:
                return city
        
        return None
    
    def _extract_district_from_address(self, address: str, province: str = None) -> Optional[str]:
        """从地址中提取区县"""
        if not address:
            return None
        
        # 各种区县模式
        patterns = [
            r'([\u4e00-\u9fa5]{2,}(?:区|县|旗))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, address)
            if match:
                district = match.group(1)
                # 排除省市名称
                if district not in self.DIRECT_CITIES and '省' not in district:
                    # 如果是直辖市，返回"市+区"格式
                    if province in self.DIRECT_CITIES:
                        return f"{province}{district}"
                    return district
        
        return None
    
    # ========== 质量评分 ==========
    
    def get_quality_score(self, row: Dict) -> Tuple[int, List[str]]:
        """
        计算单行数据质量评分 (0-100)
        
        评分维度：
        - 必填字段完整性: 40分
        - 数据格式正确性: 30分
        - 逻辑合理性: 20分
        - 可选字段完整性: 10分
        
        :param row: 行数据
        :return: (score, issues_list)
        """
        score = 100
        issues = []
        
        # 1. 必填字段检查 (每缺一个扣10分，最多扣40分)
        for field in self.REQUIRED_FIELDS:
            value = row.get(field)
            if not value or str(value).strip() in ('', 'nan'):
                score -= 10
                issues.append(f'{field}缺失')
        
        # 2. 格式检查 (每个问题扣5分，最多扣30分)
        power = row.get('额定功率')
        if power is not None and str(power).strip() not in ('', 'nan'):
            try:
                power_val = float(power)
                if power_val <= self.POWER_MIN or power_val > self.POWER_MAX:
                    score -= 5
                    issues.append('功率异常')
            except (ValueError, TypeError):
                score -= 5
                issues.append('功率格式错误')
        
        # 3. 逻辑检查 (每个问题扣5分，最多扣20分)
        province = str(row.get('省份_中文', '')).strip()
        district = str(row.get('区县_中文', '')).strip()
        
        if province in self.DIRECT_CITIES:
            if district == province or not district or district == 'nan':
                score -= 5
                issues.append('直辖市区县异常')
        
        # 4. 可选字段 (每缺一个扣2分，最多扣10分)
        optional_fields = ['充电桩编号', '所属充电站编号', '充电站名称', '充电站位置', '额定功率']
        missing_optional = 0
        for field in optional_fields:
            value = row.get(field)
            if not value or str(value).strip() in ('', 'nan'):
                missing_optional += 1
        score -= min(missing_optional * 2, 10)
        
        return max(0, score), issues
    
    # ========== 核心清洗方法 ==========
    
    def clean_row(self, row: Dict, row_num: int) -> Tuple[Dict, int, List[str]]:
        """
        清洗单行数据
        
        :param row: 原始行数据
        :param row_num: 行号
        :return: (cleaned_row, quality_score, issues)
        """
        cleaned = row.copy()
        
        # 1. 运营商校验
        cleaned['运营商名称'] = self.validate_operator(row, row_num)
        
        # 2. 区域校验
        province, city, district = self.validate_region_hierarchy(row, row_num)
        cleaned['省份_中文'] = province
        cleaned['城市_中文'] = city
        cleaned['区县_中文'] = district
        
        # 3. 充电站ID校验
        station_id, is_original = self.validate_station_id(row, row_num)
        cleaned['所属充电站编号'] = station_id
        
        # 4. 功率校验
        cleaned['额定功率'] = self.validate_power(row, row_num)
        
        # 5. 经纬度校验（如果有）
        if '经度' in row or '纬度' in row:
            lng, lat = self.validate_coordinates(row, row_num)
            cleaned['经度'] = lng
            cleaned['纬度'] = lat
        
        # 6. 生成/保留UID
        if not cleaned.get('UID') or str(cleaned.get('UID')).strip() in ('', 'nan'):
            cleaned['UID'] = str(uuid.uuid4())
        
        # 7. 添加元数据
        cleaned['data_source'] = self.source_file
        cleaned['source_row_num'] = row_num
        cleaned['import_batch_id'] = self.batch_id
        cleaned['created_at'] = datetime.now()
        cleaned['updated_at'] = datetime.now()
        
        # 8. 计算质量评分
        quality_score, issues = self.get_quality_score(cleaned)
        cleaned['data_quality_score'] = quality_score
        cleaned['quality_issues'] = json.dumps(issues, ensure_ascii=False) if issues else None
        
        return cleaned, quality_score, issues
    
    def clean_batch(self, df: pd.DataFrame, start_row: int = 0) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """
        批量清洗数据
        
        :param df: 原始DataFrame
        :param start_row: 起始行号（用于日志）
        :return: (success_df, failed_df, stats)
        """
        self.stats['total_rows'] += len(df)
        
        success_rows = []
        failed_rows = []
        
        for idx, row in df.iterrows():
            row_num = start_row + idx + 1  # 行号从1开始
            row_dict = row.to_dict()
            
            try:
                cleaned_row, quality_score, issues = self.clean_row(row_dict, row_num)
                
                if quality_score >= self.QUALITY_THRESHOLD_REJECT:
                    success_rows.append(cleaned_row)
                    self.stats['success_rows'] += 1
                    
                    if quality_score < self.QUALITY_THRESHOLD_WARNING:
                        self.stats['warning_rows'] += 1
                else:
                    # 质量分过低，拒绝导入
                    failed_rows.append({**row_dict, '_reject_reason': f'质量分{quality_score}<{self.QUALITY_THRESHOLD_REJECT}'})
                    self.stats['failed_rows'] += 1
                    
                    self.log_error(row_num, '_RECORD', 'QUALITY_SCORE_TOO_LOW',
                                  f'质量评分{quality_score}低于阈值{self.QUALITY_THRESHOLD_REJECT}，拒绝导入',
                                  error_level='ERROR', row_data=row_dict)
                    
            except Exception as e:
                failed_rows.append({**row_dict, '_reject_reason': str(e)})
                self.stats['failed_rows'] += 1
                
                self.log_error(row_num, '_RECORD', 'PROCESSING_ERROR',
                              f'处理异常: {str(e)}',
                              error_level='CRITICAL', row_data=row_dict)
        
        success_df = pd.DataFrame(success_rows) if success_rows else pd.DataFrame()
        failed_df = pd.DataFrame(failed_rows) if failed_rows else pd.DataFrame()
        
        # 检查批次成功率
        if len(df) > 0:
            batch_success_rate = len(success_rows) / len(df)
            if batch_success_rate < self.SUCCESS_RATE_THRESHOLD:
                self.logger.warning(
                    f"⚠️ 批次成功率 {batch_success_rate:.1%} 低于阈值 {self.SUCCESS_RATE_THRESHOLD:.0%}！"
                )
        
        return success_df, failed_df, self.get_stats()
    
    def check_should_pause(self) -> Tuple[bool, str]:
        """
        检查是否应该暂停导入
        
        :return: (should_pause, reason)
        """
        if self.stats['total_rows'] == 0:
            return False, ''
        
        success_rate = self.stats['success_rows'] / self.stats['total_rows']
        
        if success_rate < self.SUCCESS_RATE_THRESHOLD:
            reason = f"成功率 {success_rate:.1%} 低于阈值 {self.SUCCESS_RATE_THRESHOLD:.0%}"
            self.logger.error(f"🛑 {reason}，建议暂停导入检查源数据质量")
            return True, reason
        
        return False, ''
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        total = self.stats['total_rows']
        success_rate = self.stats['success_rows'] / total if total > 0 else 0
        
        return {
            **self.stats,
            'success_rate': success_rate,
            'error_count': len(self.error_logs),
            'batch_id': self.batch_id,
        }
    
    def save_error_logs_to_db(self) -> int:
        """
        将错误日志保存到数据库
        
        :return: 保存的记录数
        """
        if not self.error_logs or not self.engine:
            return 0
        
        try:
            df_errors = pd.DataFrame(self.error_logs)
            df_errors.to_sql('evdata_import_errors', self.engine, 
                            if_exists='append', index=False)
            
            self.logger.info(f"✅ 已保存 {len(self.error_logs)} 条错误日志到数据库")
            return len(self.error_logs)
        except Exception as e:
            self.logger.error(f"❌ 保存错误日志失败: {e}")
            return 0
    
    def save_batch_summary_to_db(self) -> bool:
        """
        保存批次汇总信息到数据库
        
        :return: 是否成功
        """
        if not self.engine:
            return False
        
        try:
            stats = self.get_stats()
            summary = {
                'batch_id': self.batch_id,
                'source_file': self.source_file,
                'total_rows': stats['total_rows'],
                'success_rows': stats['success_rows'],
                'failed_rows': stats['failed_rows'],
                'warning_rows': stats['warning_rows'],
                'success_rate': round(stats['success_rate'] * 100, 2),
                'status': 'COMPLETED' if stats['success_rate'] >= self.SUCCESS_RATE_THRESHOLD else 'PAUSED',
                'pause_reason': f"成功率{stats['success_rate']:.1%}<{self.SUCCESS_RATE_THRESHOLD:.0%}" 
                               if stats['success_rate'] < self.SUCCESS_RATE_THRESHOLD else None,
                'started_at': datetime.now(),
                'completed_at': datetime.now(),
            }
            
            df_summary = pd.DataFrame([summary])
            df_summary.to_sql('evdata_import_batches', self.engine,
                             if_exists='append', index=False)
            
            self.logger.info(f"✅ 已保存批次汇总信息")
            return True
        except Exception as e:
            self.logger.error(f"❌ 保存批次汇总失败: {e}")
            return False
    
    def print_summary(self):
        """打印清洗汇总"""
        stats = self.get_stats()
        
        print("\n" + "=" * 60)
        print("📊 数据清洗汇总报告")
        print("=" * 60)
        print(f"批次ID: {self.batch_id}")
        print(f"来源文件: {self.source_file}")
        print("-" * 60)
        print(f"总记录数: {stats['total_rows']:,}")
        print(f"成功导入: {stats['success_rows']:,}")
        print(f"失败拒绝: {stats['failed_rows']:,}")
        print(f"警告记录: {stats['warning_rows']:,}")
        print(f"错误日志: {stats['error_count']:,} 条")
        print("-" * 60)
        print(f"成功率: {stats['success_rate']:.2%}")
        
        if stats['success_rate'] >= 0.98:
            print("✅ 达到98%目标！")
        elif stats['success_rate'] >= self.SUCCESS_RATE_THRESHOLD:
            print("⚠️ 高于95%阈值，但未达98%目标")
        else:
            print(f"🛑 低于95%阈值，建议检查源数据！")
        
        print("=" * 60 + "\n")
