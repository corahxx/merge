# core/condition_parser.py - 条件解析器

import re
from typing import Dict, Optional, List
from datetime import datetime, date
from core.knowledge_base import KnowledgeBase


class ConditionParser:
    """从用户问题中解析查询条件"""
    
    def __init__(self, table_name: str = 'evdata'):
        """
        初始化条件解析器
        :param table_name: 数据表名，用于查询区域级别
        """
        self.knowledge_base = KnowledgeBase
        self.table_name = table_name
    
    def parse_conditions(self, question: str) -> Dict:
        """
        从问题中解析条件
        :param question: 用户问题
        :return: 条件字典，包含date_field, start_date, end_date, operator_filter, location_filter等
        """
        conditions = {
            'date_field': None,
            'start_date': None,
            'end_date': None,
            'operator_filter': None,
            'location_filter': None,
            'parsed_conditions': []  # 存储解析到的条件描述
        }
        
        # 解析时间条件
        date_conditions = self._parse_date_conditions(question)
        if date_conditions:
            conditions.update(date_conditions)
            conditions['parsed_conditions'].append(f"时间范围: {date_conditions.get('start_date')} 至 {date_conditions.get('end_date')}")
        
        # 解析运营商条件
        operator_filter = self._parse_operators(question)
        if operator_filter:
            conditions['operator_filter'] = operator_filter
            conditions['parsed_conditions'].append(f"运营商: {', '.join(operator_filter)}")
        
        # 解析区域条件（优先使用三级区域筛选）
        region_filter = self.parse_region_filter(question)
        if region_filter:
            conditions['region_filter'] = region_filter
            # 格式化区域条件显示
            region_parts = []
            if region_filter.get('province'):
                region_parts.append(f"省份: {region_filter['province']}")
            if region_filter.get('city'):
                region_parts.append(f"城市: {region_filter['city']}")
            if region_filter.get('district'):
                region_parts.append(f"区县: {region_filter['district']}")
            if region_parts:
                conditions['parsed_conditions'].append(" | ".join(region_parts))
        
        # 兼容旧接口：也解析 location_filter（区县列表）
        location_filter = self._parse_locations(question)
        if location_filter:
            conditions['location_filter'] = location_filter
            # 如果已经有 region_filter，不再添加 location_filter 到显示
            if not region_filter:
                conditions['parsed_conditions'].append(f"区域: {', '.join(location_filter)}")
        
        return conditions
    
    def _parse_date_conditions(self, question: str) -> Dict:
        """解析日期条件"""
        conditions = {}
        
        # 日期格式模式：2024年、2024/1/1、2024-01-01、2024.1.1等
        # 年份模式
        year_pattern = r'(\d{4})年'
        years = re.findall(year_pattern, question)
        
        # 完整日期模式
        date_patterns = [
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # 2024/1/1 或 2024-01-01
            r'(\d{4})\.(\d{1,2})\.(\d{1,2})',      # 2024.1.1
        ]
        
        dates = []
        for pattern in date_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                try:
                    year, month, day = match
                    dates.append(date(int(year), int(month), int(day)))
                except:
                    pass
        
        # 如果找到年份，但没有具体日期，使用年份的开始和结束
        if years and not dates:
            try:
                year = int(years[0])
                conditions['start_date'] = date(year, 1, 1)
                conditions['end_date'] = date(year, 12, 31)
                conditions['date_field'] = '充电站投入使用时间'  # 默认使用充电站投入使用时间
            except:
                pass
        
        # 如果有具体日期
        if dates:
            dates.sort()
            conditions['start_date'] = dates[0]
            if len(dates) > 1:
                conditions['end_date'] = dates[-1]
            else:
                conditions['end_date'] = dates[0]
            conditions['date_field'] = '充电站投入使用时间'
        
        # 检查是否有时间相关的关键词来确定使用哪个日期字段
        if '生产日期' in question or '生产' in question:
            conditions['date_field'] = '充电桩生产日期'
        elif '投入使用' in question or '投入' in question:
            conditions['date_field'] = '充电站投入使用时间'
        elif not conditions.get('date_field'):
            conditions['date_field'] = '充电站投入使用时间'  # 默认
        
        # 检查是否有"最近"、"今年"、"去年"等时间表述
        if '最近' in question or '近' in question:
            if not conditions.get('start_date'):
                # 最近一年
                today = date.today()
                conditions['start_date'] = date(today.year - 1, today.month, today.day)
                conditions['end_date'] = today
                conditions['date_field'] = '充电站投入使用时间'
        elif '今年' in question:
            today = date.today()
            conditions['start_date'] = date(today.year, 1, 1)
            conditions['end_date'] = today
            conditions['date_field'] = '充电站投入使用时间'
        elif '去年' in question:
            today = date.today()
            conditions['start_date'] = date(today.year - 1, 1, 1)
            conditions['end_date'] = date(today.year - 1, 12, 31)
            conditions['date_field'] = '充电站投入使用时间'
        
        return conditions if conditions.get('start_date') or conditions.get('end_date') else {}
    
    def _parse_operators(self, question: str) -> Optional[List[str]]:
        """解析运营商条件"""
        operators = []
        
        # 从知识库中查找运营商
        for abbr, full_name in KnowledgeBase.OPERATOR_NICKNAMES.items():
            if abbr in question or full_name in question:
                if full_name not in operators:
                    operators.append(full_name)
        
        # 如果找到运营商，返回列表
        return operators if operators else None
    
    def _parse_locations(self, question: str) -> Optional[List[str]]:
        """
        解析区域条件（兼容旧接口，返回区县列表）
        """
        locations = []
        
        # 从知识库中查找区域
        for abbr, full_name in KnowledgeBase.LOCATION_NICKNAMES.items():
            if abbr in question or full_name in question:
                if full_name not in locations:
                    locations.append(full_name)
        
        # 如果找到区域，返回列表
        return locations if locations else None
    
    def parse_region_filter(self, question: str) -> Optional[Dict[str, str]]:
        """
        解析区域筛选条件，判断区域级别（省份/城市/区县）
        :param question: 用户问题
        :return: 区域筛选字典，格式：{'province': 'XX省', 'city': 'XX市', 'district': 'XX区'} 或 None
        """
        # 方法1：从知识库中查找区域（优先）
        import re
        found_location = None
        # 按长度排序，优先匹配较长的简称（避免"南"匹配到"济南"之前）
        # 然后按全称长度排序（避免"哈尔滨"匹配到"哈尔滨市"之前）
        sorted_locations = sorted(
            KnowledgeBase.LOCATION_NICKNAMES.items(), 
            key=lambda x: (len(x[0]), len(x[1])),  # 先按简称长度，再按全称长度
            reverse=True
        )
        for abbr, full_name in sorted_locations:
            # 检查是否是完整的区域名称（前后是标点、空格或字符串边界）
            # 转义特殊字符
            escaped_abbr = re.escape(abbr)
            escaped_full = re.escape(full_name)
            
            # 对于单字简称（如"南"），需要更严格的验证
            # 确保它不会匹配到其他词的一部分（如"济南"中的"南"）
            if len(abbr) == 1:
                # 单字匹配：需要确保前后都不是中文字符或字母数字
                # 例如："南"不应该匹配到"济南"中的"南"
                abbr_pattern = r'(?:^|[^\u4e00-\u9fa5\w])' + escaped_abbr + r'(?:[^\u4e00-\u9fa5\w]|$)'
            else:
                # 多字匹配：使用更宽松的匹配，允许作为独立词出现
                # 匹配模式：前后是字符串边界、空格、标点，或者不是中文字符
                abbr_pattern = r'(?:^|[\s\W]|[^\u4e00-\u9fa5])' + escaped_abbr + r'(?:[\s\W]|[^\u4e00-\u9fa5]|$)'
            
            # 全称匹配：前后不是中文字符即可
            full_pattern = r'(?:^|[^\u4e00-\u9fa5])' + escaped_full + r'(?:[^\u4e00-\u9fa5]|$)'
            
            # 尝试匹配简称或全称
            if re.search(abbr_pattern, question) or re.search(full_pattern, question):
                found_location = full_name
                break
        
        # 如果仍然没有找到，尝试更宽松的匹配：检查问题中是否包含知识库中的简称
        # 这是最后的兜底策略，确保只要输入的是地点，就能在知识库中找到匹配
        if not found_location:
            # 按简称长度排序，优先匹配较长的简称（避免"南"匹配到"济南"之前）
            for abbr, full_name in sorted(KnowledgeBase.LOCATION_NICKNAMES.items(), key=lambda x: len(x[0]), reverse=True):
                # 如果简称在问题中出现（作为子字符串），且简称长度>=2（避免单字误匹配）
                if len(abbr) >= 2 and abbr in question:
                    # 对于多字简称，还需要检查是否是独立词（避免"哈尔滨"匹配到"黑龙江省哈尔滨"中的"哈尔滨"）
                    # 但这里我们允许部分匹配，因为用户可能只输入城市名
                    found_location = full_name
                    break
        
        # 方法2：如果知识库中没有，直接从问题中提取区域名称
        if not found_location:
            # 使用正则表达式提取包含"省"、"市"、"区"、"县"的区域名称
            import re
            # 匹配模式：XX省、XX市、XX区、XX县
            # 使用更精确的匹配，确保匹配的是完整的区域名称
            # 策略：先找到所有可能的匹配，然后验证每个匹配的前后字符，确保是完整的区域名称
            patterns = [
                (r'([\u4e00-\u9fa5]{2,4}省)', '省'),  # 匹配省份（2-4个汉字+省）
                (r'([\u4e00-\u9fa5]{2,4}市)', '市'),  # 匹配城市（2-4个汉字+市）
                (r'([\u4e00-\u9fa5]{2,6}区)', '区'),  # 匹配区（2-6个汉字+区，因为可能有"浦东新区"）
                (r'([\u4e00-\u9fa5]{2,6}县)', '县'),  # 匹配县（2-6个汉字+县）
            ]
            
            # 收集所有匹配，并验证每个匹配的前后字符
            all_matches = []
            for pattern, suffix in patterns:
                matches = re.finditer(pattern, question)
                for match in matches:
                    matched_text = match.group(1)
                    start_pos = match.start()
                    end_pos = match.end()
                    
                    # 检查匹配的前一个字符（如果存在）
                    # 如果前一个字符是中文，说明匹配可能包含了前面的字（如"下哈尔滨市"中的"下"）
                    # 我们需要确保匹配的是完整的区域名称
                    if start_pos > 0:
                        prev_char = question[start_pos - 1]
                        # 如果前一个字符是中文，检查是否应该包含它
                        if '\u4e00' <= prev_char <= '\u9fa5':
                            # 如果前一个字符是常见动词、介词等，说明匹配可能不完整
                            # 常见的前置词：查、看、找、在、到、去、下、上等
                            if prev_char in ['查', '看', '找', '在', '到', '去', '下', '上', '帮', '给', '为', '对', '向']:
                                continue
                    
                    # 检查匹配的后一个字符（如果存在）
                    if end_pos < len(question):
                        next_char = question[end_pos]
                        # 如果后一个字符是中文且不是常见助词，说明匹配可能不完整
                        if '\u4e00' <= next_char <= '\u9fa5' and next_char not in ['的', '地', '得', '和', '与', '及']:
                            continue
                    
                    # 验证通过，添加到候选列表
                    all_matches.append(matched_text)
            
            if all_matches:
                # 选择最短的匹配（通常是城市名称本身，而不是包含城市名称的句子）
                found_location = min(all_matches, key=len)
        
        if not found_location:
            return None
        
        # 判断区域级别并查询数据库确认
        region_filter = self._determine_region_level(found_location)
        return region_filter
    
    def _determine_region_level(self, location_name: str) -> Optional[Dict[str, str]]:
        """
        判断区域级别（省份/城市/区县）
        :param location_name: 区域名称（可能是完整路径，如"北京市朝阳区"）
        :return: 区域筛选字典，格式：{'province': 'XX省', 'city': 'XX市', 'district': 'XX区'}
        """
        # 处理完整路径的情况（如"北京市朝阳区"）
        # 如果同时包含"省"或"市"和"区"或"县"，说明是区县级别
        if ('省' in location_name or '市' in location_name) and ('区' in location_name or '县' in location_name):
            # 提取省份、城市、区县信息
            parts = self._extract_region_parts(location_name)
            if parts:
                return parts
        
        # 优先检查"市"（城市级别优先于省份级别）
        # 如果包含"市"，可能是城市
        if '市' in location_name:
            # 如果后面还有区县信息，提取完整路径
            if '区' in location_name or '县' in location_name:
                parts = self._extract_region_parts(location_name)
                if parts:
                    return parts
            
            # 如果同时包含"省"和"市"（如"山东省济南市"），提取城市部分
            if '省' in location_name:
                # 找到"省"的位置，提取省后面的部分（城市）
                province_end = location_name.find('省') + 1
                city_part = location_name[province_end:].strip()
                # 如果城市部分以"市"结尾，返回城市级别（使用城市部分）
                if city_part.endswith('市'):
                    return {'city': city_part}
                # 如果城市部分不完整，尝试查找"市"的位置
                city_end = location_name.find('市') + 1
                if city_end > province_end:
                    city_name = location_name[province_end:city_end]
                    return {'city': city_name}
                # 否则返回完整路径（包含省份和城市）
                return {'city': location_name}
            else:
                # 只包含"市"，直接作为城市处理
                return {'city': location_name}
        
        # 如果包含"省"，可能是省份
        if '省' in location_name:
            # 检查是否是直辖市（北京、上海、天津、重庆）
            if any(city in location_name for city in ['北京', '上海', '天津', '重庆']):
                # 直辖市，既是省份也是城市
                # 如果后面还有区县信息，需要进一步提取
                if '区' in location_name or '县' in location_name:
                    parts = self._extract_region_parts(location_name)
                    if parts:
                        return parts
                return {
                    'province': location_name,
                    'city': location_name
                }
            else:
                # 普通省份
                return {'province': location_name}
        
        # 如果包含"区"或"县"，可能是区县
        if '区' in location_name or '县' in location_name:
            return {'district': location_name}
        
        # 如果都不匹配，尝试查询数据库确定级别
        return self._query_region_level_from_db(location_name)
    
    def _extract_region_parts(self, location_name: str) -> Optional[Dict[str, str]]:
        """
        从完整区域名称中提取省份、城市、区县信息
        例如："北京市朝阳区" -> {'province': '北京市', 'city': '北京市', 'district': '北京市朝阳区'}
        :param location_name: 完整区域名称
        :return: 区域筛选字典
        """
        result = {}
        
        # 提取省份
        if '省' in location_name:
            # 找到"省"的位置
            province_end = location_name.find('省') + 1
            province = location_name[:province_end]
            result['province'] = province
        elif any(city in location_name for city in ['北京', '上海', '天津', '重庆']):
            # 直辖市，省份和城市相同
            if '市' in location_name:
                city_end = location_name.find('市') + 1
                city_name = location_name[:city_end]
                result['province'] = city_name
                result['city'] = city_name
        
        # 提取城市
        if '市' in location_name and 'city' not in result:
            city_end = location_name.find('市') + 1
            city_name = location_name[:city_end]
            result['city'] = city_name
            # 如果是直辖市，省份也是这个
            if any(city in city_name for city in ['北京', '上海', '天津', '重庆']):
                result['province'] = city_name
        
        # 提取区县（完整路径）
        if '区' in location_name or '县' in location_name:
            result['district'] = location_name
        
        return result if result else None
    
    def _query_region_level_from_db(self, location_name: str) -> Optional[Dict[str, str]]:
        """
        从数据库查询区域级别
        :param location_name: 区域名称
        :return: 区域筛选字典
        """
        try:
            from sqlalchemy import text
            from utils.db_utils import create_db_engine
            
            engine = create_db_engine(echo=False)  # 使用统一工具函数
            
            # 转义单引号，防止SQL注入
            escaped_location = location_name.replace("'", "''")
            
            # 先查询省份
            query_province = text(f"""
                SELECT DISTINCT `省份_中文` 
                FROM `{self.table_name}` 
                WHERE `省份_中文` = '{escaped_location}' 
                LIMIT 1
            """)
            with engine.connect() as conn:
                result = conn.execute(query_province)
                if result.fetchone():
                    return {'province': location_name}
            
            # 再查询城市
            query_city = text(f"""
                SELECT DISTINCT `城市_中文` 
                FROM `{self.table_name}` 
                WHERE `城市_中文` = '{escaped_location}' 
                LIMIT 1
            """)
            with engine.connect() as conn:
                result = conn.execute(query_city)
                if result.fetchone():
                    return {'city': location_name}
            
            # 最后查询区县
            query_district = text(f"""
                SELECT DISTINCT `区县_中文` 
                FROM `{self.table_name}` 
                WHERE `区县_中文` = '{escaped_location}' 
                LIMIT 1
            """)
            with engine.connect() as conn:
                result = conn.execute(query_district)
                if result.fetchone():
                    return {'district': location_name}
            
            # 如果都没找到，返回None
            return None
            
        except Exception as e:
            # 查询失败，使用默认规则
            # 如果包含"市"，假设是城市
            if '市' in location_name:
                return {'city': location_name}
            # 如果包含"区"或"县"，假设是区县
            elif '区' in location_name or '县' in location_name:
                return {'district': location_name}
            # 否则返回None
            return None
    
    def format_conditions_summary(self, conditions: Dict) -> str:
        """格式化条件摘要"""
        if not conditions.get('parsed_conditions'):
            return "无特定筛选条件（使用全量数据）"
        
        return "筛选条件：" + "；".join(conditions['parsed_conditions'])

