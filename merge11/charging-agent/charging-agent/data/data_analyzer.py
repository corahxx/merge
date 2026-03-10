# data/data_analyzer.py - 数据分析器（Pandas优化版）

import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
from sqlalchemy import text
from utils.db_utils import get_shared_engine
import streamlit as st


class DataAnalyzer:
    """
    数据分析器（Pandas优化版）
    
    优先使用 PandasDataService 的内存缓存（毫秒级响应），
    如果缓存不可用，降级使用SQL查询。
    """
    
    def __init__(self, table_name: str = 'table2509ev', verbose: bool = True):
        """
        初始化数据分析器
        :param table_name: 数据表名
        :param verbose: 是否显示详细日志
        """
        self.table_name = table_name
        self.verbose = verbose
        
        # 使用全局共享引擎（连接池优化）
        self.engine = get_shared_engine()
        
        # Pandas数据服务（懒加载）
        self._pandas_service = None
    
    @property
    def pandas_service(self):
        """获取Pandas数据服务（懒加载）"""
        if self._pandas_service is None:
            try:
                from services.pandas_data_service import PandasDataService
                self._pandas_service = PandasDataService.get_instance()
            except ImportError:
                self._pandas_service = None
        return self._pandas_service
    
    def _build_where_clause(self, date_field: Optional[str] = None, 
                           start_date: Optional[date] = None,
                           end_date: Optional[date] = None, 
                           operator_filter: Optional[List[str]] = None,
                           location_filter: Optional[List[str]] = None, 
                           power_filter: Optional[List[str]] = None,
                           power_op: Optional[str] = None,
                           power_value: Optional[float] = None,
                           power_value_min: Optional[float] = None,
                           power_value_max: Optional[float] = None,
                           region_filter: Optional[Dict[str, str]] = None,
                           charge_type_filter: Optional[List[str]] = None,
                           include_deleted: bool = False) -> str:
        """
        构建WHERE条件子句（复用逻辑）
        :param include_deleted: 是否包含已删除数据（默认False，排除已软删除数据）
        :return: WHERE子句字符串（包含WHERE关键字）或空字符串
        """
        where_conditions = []
        
        # 默认过滤已删除数据（使用is_active高效过滤）
        if not include_deleted:
            where_conditions.append("is_active = 1")
        
        # 日期筛选
        if date_field and (start_date or end_date):
            if date_field == '充电桩生产日期':
                if start_date:
                    where_conditions.append(f"`{date_field}` >= '{start_date}'")
                if end_date:
                    where_conditions.append(f"`{date_field}` <= '{end_date}'")
            elif date_field == '充电站投入使用时间':
                date_conditions = [f"`{date_field}` IS NOT NULL", f"`{date_field}` != ''"]
                normalized_date_expr = (
                    f"CONCAT("
                    f"SUBSTRING_INDEX(`{date_field}`, '/', 1), '-', "
                    f"LPAD(SUBSTRING_INDEX(SUBSTRING_INDEX(`{date_field}`, '/', 2), '/', -1), 2, '0'), '-', "
                    f"LPAD(SUBSTRING_INDEX(`{date_field}`, '/', -1), 2, '0')"
                    f")"
                )
                if start_date:
                    date_conditions.append(f"CAST({normalized_date_expr} AS DATE) >= '{start_date}'")
                if end_date:
                    date_conditions.append(f"CAST({normalized_date_expr} AS DATE) <= '{end_date}'")
                where_conditions.append("(" + " AND ".join(date_conditions) + ")")
        
        # 运营商筛选
        if operator_filter and len(operator_filter) > 0:
            escaped_operators = [f"'{op.replace(chr(39), chr(39)+chr(39))}'" for op in operator_filter]
            where_conditions.append(f"`运营商名称` IN ({', '.join(escaped_operators)})")
        
        # 三级区域筛选
        if region_filter:
            if region_filter.get('province'):
                province = region_filter['province'].replace("'", "''")
                where_conditions.append(f"`省份_中文` = '{province}'")
            if region_filter.get('city'):
                city = region_filter['city'].replace("'", "''")
                where_conditions.append(f"`城市_中文` = '{city}'")
            if region_filter.get('district'):
                district = region_filter['district'].replace("'", "''")
                where_conditions.append(f"`区县_中文` = '{district}'")
        elif location_filter and len(location_filter) > 0:
            escaped_locations = [f"'{loc.replace(chr(39), chr(39)+chr(39))}'" for loc in location_filter]
            where_conditions.append(f"`区县_中文` IN ({', '.join(escaped_locations)})")
        
        # 功率筛选
        if power_filter and len(power_filter) > 0:
            power_conditions = []
            for power_range in power_filter:
                if power_range == '≤7kW（慢充）':
                    power_conditions.append("(`额定功率` <= 7 AND `额定功率` IS NOT NULL)")
                elif power_range == '7-30kW（小功率）':
                    power_conditions.append("(`额定功率` > 7 AND `额定功率` <= 30)")
                elif power_range == '30-60kW（中功率）':
                    power_conditions.append("(`额定功率` > 30 AND `额定功率` <= 60)")
                elif power_range == '60-120kW（大功率）':
                    power_conditions.append("(`额定功率` > 60 AND `额定功率` <= 120)")
                elif power_range == '>120kW（超快充）':
                    power_conditions.append("(`额定功率` > 120)")
            if power_conditions:
                where_conditions.append("(" + " OR ".join(power_conditions) + ")")
        # 功率比较：大于/小于/等于/大于等于/小于等于 + 数值；或 介于 + 最小值～最大值
        if power_op == '介于' and power_value_min is not None and power_value_max is not None:
            lo, hi = float(power_value_min), float(power_value_max)
            if lo > hi:
                lo, hi = hi, lo
            where_conditions.append(f"(`额定功率` >= {lo} AND `额定功率` <= {hi} AND `额定功率` IS NOT NULL)")
        elif power_op in ('大于', '小于', '等于', '大于等于', '小于等于') and power_value is not None:
            pv = float(power_value)
            if power_op == '大于':
                where_conditions.append(f"(`额定功率` > {pv} AND `额定功率` IS NOT NULL)")
            elif power_op == '小于':
                where_conditions.append(f"(`额定功率` < {pv} AND `额定功率` IS NOT NULL)")
            elif power_op == '等于':
                where_conditions.append(f"(`额定功率` = {pv} AND `额定功率` IS NOT NULL)")
            elif power_op == '大于等于':
                where_conditions.append(f"(`额定功率` >= {pv} AND `额定功率` IS NOT NULL)")
            elif power_op == '小于等于':
                where_conditions.append(f"(`额定功率` <= {pv} AND `额定功率` IS NOT NULL)")
        
        # 充电类型筛选
        if charge_type_filter and len(charge_type_filter) > 0:
            escaped_types = [f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in charge_type_filter]
            where_conditions.append(f"`充电桩类型_转换` IN ({', '.join(escaped_types)})")
        
        return " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    def _get_count_by_field_sql(self, field: str, where_clause: str, limit: int = 100) -> Dict:
        """
        使用SQL聚合查询按字段统计数量
        :param field: 统计字段名
        :param where_clause: WHERE条件子句
        :param limit: 返回结果数量限制
        :return: {字段值: 数量} 字典
        """
        query = f"""
            SELECT `{field}`, COUNT(*) as cnt 
            FROM `{self.table_name}` 
            {where_clause}
            GROUP BY `{field}` 
            ORDER BY cnt DESC
            LIMIT {limit}
        """
        try:
            df = pd.read_sql(text(query), self.engine)
            return dict(zip(df[field], df['cnt']))
        except Exception as e:
            if self.verbose:
                print(f"⚠️ 字段 {field} 聚合查询失败: {e}")
            return {}
    
    def compare_datasets(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                        key_column: str = '充电桩编号') -> Dict:
        """
        比对两个数据集
        :param df1: 数据集1
        :param df2: 数据集2
        :param key_column: 用于比对的唯一键列
        :return: 比对结果字典
        """
        result = {
            'total_in_df1': len(df1),
            'total_in_df2': len(df2),
            'common_records': 0,
            'only_in_df1': 0,
            'only_in_df2': 0,
            'differences': []
        }
        
        if key_column not in df1.columns or key_column not in df2.columns:
            raise ValueError(f"键列 '{key_column}' 不存在于数据集中")
        
        # 获取键集合
        keys1 = set(df1[key_column].astype(str))
        keys2 = set(df2[key_column].astype(str))
        
        # 计算交集和差集
        common_keys = keys1 & keys2
        only_in_df1 = keys1 - keys2
        only_in_df2 = keys2 - keys1
        
        result['common_records'] = len(common_keys)
        result['only_in_df1'] = len(only_in_df1)
        result['only_in_df2'] = len(only_in_df2)
        
        # 比对共同记录的数据差异
        if common_keys:
            df1_common = df1[df1[key_column].astype(str).isin(common_keys)].set_index(key_column)
            df2_common = df2[df2[key_column].astype(str).isin(common_keys)].set_index(key_column)
            
            # 找出有差异的记录
            for key in list(common_keys)[:100]:  # 限制检查前100条，避免过慢
                try:
                    row1 = df1_common.loc[str(key)]
                    row2 = df2_common.loc[str(key)]
                    
                    # 比较每个字段
                    differences = []
                    for col in df1_common.columns:
                        if col in df2_common.columns:
                            val1 = str(row1.get(col, ''))
                            val2 = str(row2.get(col, ''))
                            if val1 != val2:
                                differences.append({
                                    'field': col,
                                    'df1_value': val1,
                                    'df2_value': val2
                                })
                    
                    if differences:
                        result['differences'].append({
                            'key': key,
                            'differences': differences
                        })
                except:
                    pass
        
        if self.verbose:
            print("\n" + "="*50)
            print("📊 数据集比对结果")
            print("="*50)
            print(f"数据集1记录数: {result['total_in_df1']}")
            print(f"数据集2记录数: {result['total_in_df2']}")
            print(f"共同记录: {result['common_records']}")
            print(f"仅在数据集1: {result['only_in_df1']}")
            print(f"仅在数据集2: {result['only_in_df2']}")
            print(f"数据差异记录: {len(result['differences'])}")
            print("="*50 + "\n")
        
        return result
    
    def merge_datasets(self, df1: pd.DataFrame, df2: pd.DataFrame,
                      how: str = 'outer', on: Optional[str] = None,
                      key_column: str = '充电桩编号') -> pd.DataFrame:
        """
        整合两个数据集
        :param df1: 数据集1
        :param df2: 数据集2
        :param how: 合并方式 ('left', 'right', 'outer', 'inner')
        :param on: 合并键列（如果为None，使用key_column）
        :param key_column: 默认键列
        :return: 合并后的DataFrame
        """
        merge_key = on if on else key_column
        
        if merge_key not in df1.columns or merge_key not in df2.columns:
            raise ValueError(f"键列 '{merge_key}' 不存在于数据集中")
        
        # 执行合并
        merged_df = pd.merge(df1, df2, on=merge_key, how=how, suffixes=('_1', '_2'))
        
        if self.verbose:
            print(f"✅ 数据集整合完成: {len(merged_df)} 行")
        
        return merged_df
    
    def get_statistics(self, group_by: Optional[str] = None, limit: Optional[int] = None,
                      date_field: Optional[str] = None, start_date: Optional[date] = None,
                      end_date: Optional[date] = None, operator_filter: Optional[List[str]] = None,
                      location_filter: Optional[List[str]] = None, power_filter: Optional[List[str]] = None,
                      power_op: Optional[str] = None, power_value: Optional[float] = None,
                      power_value_min: Optional[float] = None, power_value_max: Optional[float] = None,
                      region_filter: Optional[Dict[str, str]] = None, charge_type_filter: Optional[List[str]] = None) -> Dict:
        """
        获取数据统计信息（优先使用Pandas缓存，毫秒级响应）
        :param group_by: 分组字段（如 '运营商名称', '区县_中文', '所属充电站编号' 等）
        :param limit: 分组统计返回数量限制（默认100条，对主统计无影响）
        :param date_field: 日期字段名（'充电站投入使用时间' 或 '充电桩生产日期'）
        :param start_date: 起始日期
        :param end_date: 结束日期
        :param operator_filter: 运营商名称列表，用于筛选（None表示不筛选）
        :param location_filter: 区域名称列表，用于筛选（None表示不筛选，兼容旧接口）
        :param power_filter: 功率区间列表，用于筛选（None表示不筛选）
        :param region_filter: 三级区域筛选字典，格式：{'province': 'XX省', 'city': 'XX市', 'district': 'XX区'}（None表示不筛选）
        :param charge_type_filter: 充电类型筛选列表
        :return: 统计结果字典
        """
        # 使用Pandas缓存（自动触发加载，首次约30秒，后续毫秒级）
        if self.pandas_service:
            return self._get_statistics_from_pandas(
                group_by=group_by, limit=limit,
                date_field=date_field, start_date=start_date, end_date=end_date,
                operator_filter=operator_filter, location_filter=location_filter,
                power_filter=power_filter, power_op=power_op, power_value=power_value,
                power_value_min=power_value_min, power_value_max=power_value_max,
                region_filter=region_filter, charge_type_filter=charge_type_filter
            )
        
        # 降级使用SQL查询（仅当pandas_service不可用时）
        return self._get_statistics_from_sql(
            group_by=group_by, limit=limit,
            date_field=date_field, start_date=start_date, end_date=end_date,
            operator_filter=operator_filter, location_filter=location_filter,
            power_filter=power_filter, power_op=power_op, power_value=power_value,
            power_value_min=power_value_min, power_value_max=power_value_max,
            region_filter=region_filter, charge_type_filter=charge_type_filter
        )
    
    def _get_statistics_from_pandas(self, group_by: Optional[str] = None, limit: Optional[int] = None,
                                   date_field: Optional[str] = None, start_date: Optional[date] = None,
                                   end_date: Optional[date] = None, operator_filter: Optional[List[str]] = None,
                                   location_filter: Optional[List[str]] = None, power_filter: Optional[List[str]] = None,
                                   power_op: Optional[str] = None, power_value: Optional[float] = None,
                                   power_value_min: Optional[float] = None, power_value_max: Optional[float] = None,
                                   region_filter: Optional[Dict[str, str]] = None, charge_type_filter: Optional[List[str]] = None) -> Dict:
        """从Pandas缓存获取统计（毫秒级）"""
        # 构建筛选条件
        filters = {}
        if region_filter:
            filters['province'] = region_filter.get('province')
            filters['city'] = region_filter.get('city')
            filters['district'] = region_filter.get('district')
        if operator_filter:
            filters['operators'] = operator_filter
        if charge_type_filter:
            filters['charge_types'] = charge_type_filter
        if date_field:
            filters['date_field'] = date_field
            filters['start_date'] = start_date
            filters['end_date'] = end_date
        
        # 功率筛选转换
        if power_filter:
            for pr in power_filter:
                if pr == '≤7kW（慢充）':
                    filters['power_max'] = 7
                elif pr == '>120kW（超快充）':
                    filters['power_min'] = 120
            # 注意：复杂功率筛选降级到SQL
            if '7-30kW（小功率）' in power_filter or '30-60kW（中功率）' in power_filter or '60-120kW（大功率）' in power_filter:
                # 复杂功率筛选降级到SQL
                return self._get_statistics_from_sql(
                    group_by=group_by, limit=limit,
                    date_field=date_field, start_date=start_date, end_date=end_date,
                    operator_filter=operator_filter, location_filter=location_filter,
                    power_filter=power_filter, power_op=power_op, power_value=power_value,
                    power_value_min=power_value_min, power_value_max=power_value_max,
                    region_filter=region_filter, charge_type_filter=charge_type_filter
                )
        if power_op and power_op != '无':
            if power_op == '介于' and power_value_min is not None and power_value_max is not None:
                filters['power_op'] = '介于'
                filters['power_value_min'] = power_value_min
                filters['power_value_max'] = power_value_max
            elif power_op in ('大于', '小于', '等于', '大于等于', '小于等于') and power_value is not None:
                filters['power_op'] = power_op
                filters['power_value'] = power_value
        
        # 调用PandasDataService
        group_limit = limit if (limit is not None and limit > 0) else 100
        stats = self.pandas_service.get_statistics(filters=filters if filters else None, group_limit=group_limit)
        
        # 添加自定义分组统计
        if group_by:
            df = self.pandas_service.get_dataframe()
            if filters:
                df = self.pandas_service._apply_filters(df, filters)
            if group_by in df.columns:
                stats['grouped_stats'] = {group_by: self.pandas_service._group_count(df, group_by, group_limit)}
        
        if self.verbose:
            self._print_statistics(stats)
        
        return stats
    
    def _get_statistics_from_sql(self, group_by: Optional[str] = None, limit: Optional[int] = None,
                                date_field: Optional[str] = None, start_date: Optional[date] = None,
                                end_date: Optional[date] = None, operator_filter: Optional[List[str]] = None,
                                location_filter: Optional[List[str]] = None, power_filter: Optional[List[str]] = None,
                                power_op: Optional[str] = None, power_value: Optional[float] = None,
                                power_value_min: Optional[float] = None, power_value_max: Optional[float] = None,
                                region_filter: Optional[Dict[str, str]] = None, charge_type_filter: Optional[List[str]] = None) -> Dict:
        """从SQL获取统计（降级方案）"""
        try:
            # 构建WHERE子句（复用方法）
            where_clause = self._build_where_clause(
                date_field=date_field, start_date=start_date, end_date=end_date,
                operator_filter=operator_filter, location_filter=location_filter,
                power_filter=power_filter, power_op=power_op, power_value=power_value,
                power_value_min=power_value_min, power_value_max=power_value_max,
                region_filter=region_filter, charge_type_filter=charge_type_filter
            )
            
            if self.verbose and where_clause:
                if date_field:
                    print(f"📅 时间筛选条件: {date_field} 在 {start_date} 到 {end_date} 之间")
                if operator_filter:
                    print(f"🏢 运营商筛选: {', '.join(operator_filter)}")
                if region_filter:
                    print(f"📍 区域筛选: {region_filter}")
                if power_filter:
                    print(f"⚡ 功率筛选: {', '.join(power_filter)}")
            
            stats = {
                'total_records': 0,
                'columns': [],
                'basic_stats': {}
            }
            
            # 1. 获取总记录数和唯一值统计（单次SQL）
            count_query = f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT `充电桩编号`) as unique_piles,
                    COUNT(DISTINCT `所属充电站编号`) as unique_stations
                FROM `{self.table_name}` {where_clause}
            """
            count_df = pd.read_sql(text(count_query), self.engine)
            stats['total_records'] = int(count_df.iloc[0]['total'])
            stats['basic_stats']['unique_piles'] = int(count_df.iloc[0]['unique_piles'])
            stats['basic_stats']['unique_stations'] = int(count_df.iloc[0]['unique_stations'])
            
            # 2. 获取列信息（一次性查询）
            col_query = f"SHOW COLUMNS FROM `{self.table_name}`"
            col_df = pd.read_sql(text(col_query), self.engine)
            stats['columns'] = col_df['Field'].tolist()
            
            # 分组统计返回数量限制
            group_limit = limit if (limit is not None and limit > 0) else 100
            
            # 3. 按运营商统计（SQL聚合）
            stats['basic_stats']['by_operator'] = self._get_count_by_field_sql('运营商名称', where_clause, group_limit)
            
            # 4. 按区域统计（根据筛选条件智能选择统计级别）
            direct_cities = ['北京市', '上海市', '天津市', '重庆市', '北京', '上海', '天津', '重庆']
            
            if region_filter:
                province = region_filter.get('province', '')
                city = region_filter.get('city', '')
                district = region_filter.get('district', '')
                
                if province in direct_cities or city in direct_cities:
                    location_field = '区县_中文'
                    location_key = 'by_location'
                elif district:
                    location_field = '区县_中文'
                    location_key = 'by_location'
                elif city:
                    location_field = '区县_中文'
                    location_key = 'by_location'
                elif province:
                    location_field = '城市_中文'
                    location_key = 'by_city'
                else:
                    location_field = '区县_中文'
                    location_key = 'by_location'
            else:
                location_field = '省份_中文'
                location_key = 'by_province'
            
            # 主区域统计
            stats['basic_stats'][location_key] = self._get_count_by_field_sql(location_field, where_clause, group_limit)
            
            # 补充其他级别区域统计（用于报告生成）
            if location_key == 'by_province':
                stats['basic_stats']['by_city'] = self._get_count_by_field_sql('城市_中文', where_clause, group_limit)
                stats['basic_stats']['by_location'] = self._get_count_by_field_sql('区县_中文', where_clause, group_limit)
            elif location_key == 'by_city':
                stats['basic_stats']['by_location'] = self._get_count_by_field_sql('区县_中文', where_clause, group_limit)
            elif location_key == 'by_location':
                stats['basic_stats']['by_city'] = self._get_count_by_field_sql('城市_中文', where_clause, group_limit)
                stats['basic_stats']['by_province'] = self._get_count_by_field_sql('省份_中文', where_clause, group_limit)
            
            # 5. 按类型统计（SQL聚合）
            stats['basic_stats']['by_type'] = self._get_count_by_field_sql('充电桩类型_转换', where_clause, group_limit)
            
            # 6. 按充电站统计（SQL聚合，限制返回数量）
            stats['basic_stats']['by_station'] = self._get_count_by_field_sql('所属充电站编号', where_clause, group_limit)
            
            # 7. 额定功率统计（SQL聚合）
            # 构建功率查询的WHERE条件（需要合并原有条件和功率条件）
            power_condition = "`额定功率` IS NOT NULL AND `额定功率` > 0"
            if where_clause:
                # 已有WHERE子句，用AND连接
                power_where = f"{where_clause} AND {power_condition}"
            else:
                # 无WHERE子句，新建WHERE
                power_where = f" WHERE {power_condition}"
            
            power_query = f"""
                SELECT 
                    COUNT(`额定功率`) as total_count,
                    AVG(`额定功率`) as mean_val,
                    MIN(`额定功率`) as min_val,
                    MAX(`额定功率`) as max_val,
                    STDDEV(`额定功率`) as std_val,
                    SUM(CASE WHEN `额定功率` <= 7 THEN 1 ELSE 0 END) as slow_charge,
                    SUM(CASE WHEN `额定功率` > 7 AND `额定功率` <= 30 THEN 1 ELSE 0 END) as small_power,
                    SUM(CASE WHEN `额定功率` > 30 AND `额定功率` <= 60 THEN 1 ELSE 0 END) as medium_power,
                    SUM(CASE WHEN `额定功率` > 60 AND `额定功率` <= 120 THEN 1 ELSE 0 END) as large_power,
                    SUM(CASE WHEN `额定功率` > 120 THEN 1 ELSE 0 END) as super_fast
                FROM `{self.table_name}` {power_where}
            """
            power_df = pd.read_sql(text(power_query), self.engine)
            if not power_df.empty and power_df.iloc[0]['total_count'] > 0:
                row = power_df.iloc[0]
                power_stats = {
                    'total_count': int(row['total_count']),
                    'mean': float(row['mean_val']) if row['mean_val'] else 0.0,
                    'min': float(row['min_val']) if row['min_val'] else 0.0,
                    'max': float(row['max_val']) if row['max_val'] else 0.0,
                    'std': float(row['std_val']) if row['std_val'] else 0.0,
                    'by_range': {
                        '≤7kW（慢充）': int(row['slow_charge']),
                        '7-30kW（小功率）': int(row['small_power']),
                        '30-60kW（中功率）': int(row['medium_power']),
                        '60-120kW（大功率）': int(row['large_power']),
                        '>120kW（超快充）': int(row['super_fast'])
                    }
                }
                # 计算中位数（需要单独查询）
                median_query = f"""
                    SELECT `额定功率` FROM `{self.table_name}` {power_where}
                    ORDER BY `额定功率`
                    LIMIT 1 OFFSET {int(row['total_count']) // 2}
                """
                try:
                    median_df = pd.read_sql(text(median_query), self.engine)
                    power_stats['median'] = float(median_df.iloc[0]['额定功率']) if not median_df.empty else power_stats['mean']
                except:
                    power_stats['median'] = power_stats['mean']
                
                stats['basic_stats']['by_power'] = power_stats
            
            # 8. 自定义分组统计
            if group_by:
                stats['grouped_stats'] = {group_by: self._get_count_by_field_sql(group_by, where_clause, group_limit)}
            
            if self.verbose:
                self._print_statistics(stats)
            
            return stats
            
        except Exception as e:
            if self.verbose:
                print(f"❌ 获取统计信息失败: {str(e)}")
            return {'error': str(e)}
    
    def get_operator_list(self) -> List[str]:
        """
        获取所有运营商名称列表（自动使用Pandas缓存）
        :return: 运营商名称列表
        """
        try:
            # 使用Pandas缓存（自动触发加载）
            if self.pandas_service:
                return self.pandas_service.get_operators()
            
            # 降级使用SQL（仅当pandas_service不可用时）
            query = f"""SELECT DISTINCT `运营商名称` FROM `{self.table_name}` 
                       WHERE `运营商名称` IS NOT NULL AND `运营商名称` != '' 
                       AND is_active = 1
                       ORDER BY `运营商名称`"""
            df = pd.read_sql(query, self.engine)
            operators = df['运营商名称'].tolist()
            return operators
        except Exception as e:
            if self.verbose:
                print(f"获取运营商列表失败: {str(e)}")
            return []
    
    def compare_with_database(self, df: pd.DataFrame, 
                             key_column: str = '充电桩编号') -> Dict:
        """
        将DataFrame与数据库中的数据比对（排除已删除数据）
        :param df: 要比对的DataFrame
        :param key_column: 唯一键列
        :return: 比对结果
        """
        try:
            # 从数据库读取数据（排除已删除数据）
            query = f"SELECT * FROM {self.table_name} WHERE is_active = 1"
            db_df = pd.read_sql(query, self.engine)
            
            # 使用compare_datasets方法比对
            return self.compare_datasets(df, db_df, key_column)
            
        except Exception as e:
            return {'error': f"数据库比对失败: {str(e)}"}
    
    def find_duplicates(self, key_column: str = '充电桩编号') -> pd.DataFrame:
        """
        查找数据库中的重复记录（排除已删除数据）
        :param key_column: 用于检测重复的键列
        :return: 重复记录DataFrame
        """
        try:
            query = f"""
                SELECT *, COUNT(*) as duplicate_count
                FROM {self.table_name}
                WHERE is_active = 1
                GROUP BY {key_column}
                HAVING COUNT(*) > 1
            """
            duplicates = pd.read_sql(query, self.engine)
            
            if self.verbose:
                print(f"🔍 发现 {len(duplicates)} 条重复记录")
            
            return duplicates
            
        except Exception as e:
            if self.verbose:
                print(f"❌ 查找重复记录失败: {str(e)}")
            return pd.DataFrame()
    
    def get_data_quality_report(self) -> Dict:
        """
        生成数据质量报告（自动使用Pandas缓存）
        :return: 数据质量报告字典
        """
        try:
            # 使用Pandas缓存（自动触发加载，全量数据分析更准确）
            if self.pandas_service:
                df = self.pandas_service.get_dataframe()
            else:
                # 降级使用SQL（仅当pandas_service不可用时）
                query = f"SELECT * FROM {self.table_name} WHERE is_active = 1 LIMIT 10000"
                df = pd.read_sql(query, self.engine)
            
            report = {
                'total_records': len(df),
                'columns': {},
                'data_quality_score': 0.0,
                'source': 'pandas_cache' if (self.pandas_service and self.pandas_service.is_loaded()) else 'sql_sample'
            }
            
            # 分析每个字段
            quality_scores = []
            for col in df.columns:
                col_info = {
                    'null_count': int(df[col].isnull().sum()),
                    'null_percentage': float((df[col].isnull().sum() / len(df)) * 100) if len(df) > 0 else 0,
                    'unique_count': int(df[col].nunique()),
                    'data_type': str(df[col].dtype)
                }
                
                # 计算字段质量分数（0-100）
                null_score = max(0, 100 - col_info['null_percentage'])
                quality_scores.append(null_score)
                
                report['columns'][col] = col_info
            
            # 计算总体质量分数
            if quality_scores:
                report['data_quality_score'] = sum(quality_scores) / len(quality_scores)
            
            if self.verbose:
                print("\n" + "="*50)
                print("数据质量报告")
                print("="*50)
                print(f"总记录数: {report['total_records']}")
                print(f"数据质量分数: {report['data_quality_score']:.2f}/100")
                print("="*50 + "\n")
            
            return report
            
        except Exception as e:
            return {'error': f"生成数据质量报告失败: {str(e)}"}
    
    def _print_statistics(self, stats: Dict):
        """打印统计信息"""
        print("\n" + "="*50)
        print("📊 数据统计")
        print("="*50)
        print(f"总记录数: {stats.get('total_records', 0)}")
        if 'basic_stats' in stats:
            bs = stats['basic_stats']
            if 'unique_piles' in bs:
                print(f"唯一充电桩数: {bs['unique_piles']}")
        print("="*50 + "\n")

