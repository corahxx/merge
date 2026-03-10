# handlers/data_analysis_handler.py - 数据分析处理（Pandas优化版）

from typing import Dict, Optional, List, Tuple
from datetime import date
import pandas as pd
import plotly.express as px
import streamlit as st
import warnings
import hashlib
import json

from data.data_processor import DataProcessor
from data.error_handler import ErrorHandler

# 过滤 Altair 类型推断警告（当数据为空时）
warnings.filterwarnings("ignore", message=".*don't know how to infer vegalite type from 'empty'.*")


# ========== Pandas数据服务（懒加载） ==========

def _get_pandas_service():
    """获取Pandas数据服务单例"""
    try:
        from services.pandas_data_service import PandasDataService
        return PandasDataService.get_instance()
    except ImportError:
        return None


# ========== 缓存辅助函数 ==========

def _make_cache_key(*args) -> str:
    """生成缓存键（将参数转换为可哈希的字符串）"""
    def serialize(obj):
        if obj is None:
            return "None"
        if isinstance(obj, (list, tuple)):
            return json.dumps(sorted(obj) if isinstance(obj, list) else list(obj))
        if isinstance(obj, dict):
            return json.dumps(obj, sort_keys=True)
        if isinstance(obj, date):
            return obj.isoformat()
        return str(obj)
    
    key_parts = [serialize(arg) for arg in args]
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()


@st.cache_data(ttl=300, show_spinner=False)  # 缓存5分钟
def _cached_get_statistics(table_name: str, cache_key: str,
                           group_by: Optional[str],
                           limit: Optional[int],
                           date_field: Optional[str],
                           start_date_str: Optional[str],
                           end_date_str: Optional[str],
                           operator_filter_json: Optional[str],
                           power_filter_json: Optional[str],
                           power_op: Optional[str],
                           power_value: Optional[float],
                           power_value_min: Optional[float],
                           power_value_max: Optional[float],
                           region_filter_json: Optional[str],
                           charge_type_filter_json: Optional[str]) -> Dict:
    """
    带缓存的统计查询（优先使用Pandas缓存）
    注意：参数需要是可哈希的基本类型
    """
    # 反序列化参数
    start_date = date.fromisoformat(start_date_str) if start_date_str else None
    end_date = date.fromisoformat(end_date_str) if end_date_str else None
    operator_filter = json.loads(operator_filter_json) if operator_filter_json else None
    power_filter = json.loads(power_filter_json) if power_filter_json else None
    region_filter = json.loads(region_filter_json) if region_filter_json else None
    charge_type_filter = json.loads(charge_type_filter_json) if charge_type_filter_json else None
    
    # 使用Pandas缓存（自动触发加载，首次约30秒，后续毫秒级）
    pandas_service = _get_pandas_service()
    if pandas_service:
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
        if power_op and power_op != '无':
            if power_op == '介于' and power_value_min is not None and power_value_max is not None:
                filters['power_op'] = '介于'
                filters['power_value_min'] = power_value_min
                filters['power_value_max'] = power_value_max
            elif power_op in ('大于', '小于', '等于', '大于等于', '小于等于') and power_value is not None:
                filters['power_op'] = power_op
                filters['power_value'] = power_value
        
        # 复杂功率筛选降级到SQL
        has_complex_power_filter = power_filter and any(
            p in power_filter for p in ['7-30kW（小功率）', '30-60kW（中功率）', '60-120kW（大功率）']
        )
        
        if not has_complex_power_filter:
            # 简单功率筛选可以用Pandas处理
            if power_filter:
                if '≤7kW（慢充）' in power_filter:
                    filters['power_max'] = 7
                elif '>120kW（超快充）' in power_filter:
                    filters['power_min'] = 120
            
            group_limit = limit if (limit is not None and limit > 0) else 100
            stats = pandas_service.get_statistics(filters=filters if filters else None, group_limit=group_limit)
            
            # 添加自定义分组统计
            if group_by:
                df = pandas_service.get_dataframe()
                if filters:
                    df = pandas_service._apply_filters(df, filters)
                if group_by in df.columns:
                    stats['grouped_stats'] = {group_by: pandas_service._group_count(df, group_by, group_limit)}
            
            return stats
    
    # 降级使用SQL查询
    processor = DataProcessor(table_name=table_name, verbose=False)
    return processor.get_database_statistics(
        group_by=group_by,
        limit=limit,
        date_field=date_field,
        start_date=start_date,
        end_date=end_date,
        operator_filter=operator_filter,
        power_filter=power_filter,
        power_op=power_op,
        power_value=power_value,
        power_value_min=power_value_min,
        power_value_max=power_value_max,
        region_filter=region_filter,
        charge_type_filter=charge_type_filter
    )


@st.cache_data(ttl=600, show_spinner=False)  # 缓存10分钟
def _cached_get_operator_list(table_name: str) -> List[str]:
    """带缓存的运营商列表查询（自动使用Pandas缓存）"""
    # 使用Pandas缓存（自动触发加载）
    pandas_service = _get_pandas_service()
    if pandas_service:
        return pandas_service.get_operators()
    
    # 降级使用SQL（仅当pandas_service不可用时）
    processor = DataProcessor(table_name=table_name, verbose=False)
    return processor.get_operator_list()


# ========== 主类 ==========

class DataAnalysisHandler:
    """处理数据分析统计逻辑（优化版：带缓存支持）"""
    
    def __init__(self, table_name: str):
        """
        初始化数据分析处理器
        :param table_name: 数据表名
        """
        self.table_name = table_name
        self.processor = DataProcessor(table_name=table_name, verbose=False)
    
    def get_operator_list(self) -> List[str]:
        """
        获取所有运营商名称列表（带缓存）
        :return: 运营商名称列表
        """
        try:
            return _cached_get_operator_list(self.table_name)
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "获取运营商列表")
            return []
    
    def get_statistics(self,
                      analysis_type: str,
                      limit: Optional[int] = None,
                      date_field: Optional[str] = None,
                      start_date: Optional[date] = None,
                      end_date: Optional[date] = None,
                      operator_filter: Optional[List[str]] = None,
                      power_filter: Optional[List[str]] = None,
                      power_op: Optional[str] = None,
                      power_value: Optional[float] = None,
                      power_value_min: Optional[float] = None,
                      power_value_max: Optional[float] = None,
                      region_filter: Optional[Dict[str, str]] = None,
                      charge_type_filter: Optional[List[str]] = None,
                      use_cache: bool = True) -> Dict:
        """
        获取统计数据（带缓存支持）
        :param analysis_type: 分析类型（总体统计/按运营商统计/按区域统计/按类型统计/按充电站统计）
        :param limit: 记录数限制
        :param date_field: 日期字段名
        :param start_date: 起始日期
        :param end_date: 结束日期
        :param operator_filter: 运营商名称列表，用于筛选（None表示不筛选）
        :param power_filter: 功率区间列表，用于筛选（None表示不筛选）
        :param power_op: 功率比较方式（'无'/'大于'/'小于'/'等于'/'大于等于'/'小于等于'/'介于'），单值用 power_value，介于用 power_value_min/power_value_max
        :param power_value: 功率数值 (kW)，单值比较时使用
        :param power_value_min: 功率最小值 (kW)，仅当 power_op 为「介于」时使用
        :param power_value_max: 功率最大值 (kW)，仅当 power_op 为「介于」时使用
        :param region_filter: 三级区域筛选字典，格式：{'province': 'XX省', 'city': 'XX市', 'district': 'XX区'}（None表示不筛选）
        :param charge_type_filter: 充电类型列表（'直流'或'交流'），用于筛选（None表示不筛选）
        :param use_cache: 是否使用缓存（默认True）
        :return: 统计结果字典
        """
        try:
            # 确定分组字段
            group_by = None
            if analysis_type == "按运营商统计":
                group_by = "运营商名称"
            elif analysis_type == "按区域统计":
                group_by = "区县_中文"
            elif analysis_type == "按类型统计":
                group_by = "充电桩类型_转换"
            elif analysis_type == "按充电站统计":
                group_by = "所属充电站编号"
            
            # 使用缓存或直接查询
            if use_cache:
                # 序列化参数用于缓存
                cache_key = _make_cache_key(
                    self.table_name, group_by, limit, date_field,
                    start_date, end_date, operator_filter, power_filter,
                    power_op, power_value, power_value_min, power_value_max,
                    region_filter, charge_type_filter
                )
                
                stats = _cached_get_statistics(
                    table_name=self.table_name,
                    cache_key=cache_key,
                    group_by=group_by,
                    limit=limit,
                    date_field=date_field,
                    start_date_str=start_date.isoformat() if start_date else None,
                    end_date_str=end_date.isoformat() if end_date else None,
                    operator_filter_json=json.dumps(operator_filter) if operator_filter else None,
                    power_filter_json=json.dumps(power_filter) if power_filter else None,
                    power_op=power_op,
                    power_value=power_value,
                    power_value_min=power_value_min,
                    power_value_max=power_value_max,
                    region_filter_json=json.dumps(region_filter) if region_filter else None,
                    charge_type_filter_json=json.dumps(charge_type_filter) if charge_type_filter else None
                )
            else:
                # 直接查询（不使用缓存）
                stats = self.processor.get_database_statistics(
                    group_by=group_by,
                    limit=limit,
                    date_field=date_field,
                    start_date=start_date,
                    end_date=end_date,
                    operator_filter=operator_filter,
                    power_filter=power_filter,
                    power_op=power_op,
                    power_value=power_value,
                    power_value_min=power_value_min,
                    power_value_max=power_value_max,
                    region_filter=region_filter,
                    charge_type_filter=charge_type_filter
                )
            
            # 检查返回结果
            if not isinstance(stats, dict):
                return {
                    'success': False,
                    'error': f'统计结果格式错误: 期望字典类型，实际为 {type(stats)}'
                }
            
            if 'error' in stats:
                return {
                    'success': False,
                    'error': stats['error']
                }
            
            return {
                'success': True,
                'stats': stats,
                'analysis_type': analysis_type
            }
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "统计分析")
            return {
                'success': False,
                'error': error_info.get('error_message', str(e)),
                'error_details': error_info
            }
    
    def prepare_chart_data(self, stats: Dict, analysis_type: str, 
                           region_filter: Optional[Dict[str, str]] = None) -> Dict:
        """
        准备图表数据
        :param stats: 统计数据
        :param analysis_type: 分析类型
        :param region_filter: 区域筛选条件，用于智能选择下一级统计
        :return: 图表数据字典
        """
        chart_data = None
        
        # 优先使用 grouped_stats（如果存在）
        if 'grouped_stats' in stats and stats['grouped_stats']:
            for field, group_dict in stats['grouped_stats'].items():
                if group_dict:
                    df = pd.DataFrame(
                        list(group_dict.items()),
                        columns=[field, '数量']
                    )
                    df = df.sort_values('数量', ascending=False)
                    
                    # 根据字段名确定图表类型
                    if field == "运营商名称":
                        chart_type = '运营商分布'
                        df = df.head(50)  # 限制显示前50个
                    elif field == "区县_中文":
                        chart_type = '区域分布'
                        df = df.head(20)  # 限制显示前20个
                    elif field == "充电桩类型_转换":
                        chart_type = '类型分布'
                    elif field == "所属充电站编号":
                        chart_type = '充电站分布'
                        df = df.head(50)  # 限制显示前50个充电站
                    else:
                        chart_type = f'{field}分布'
                    
                    chart_data = {
                        'type': chart_type,
                        'data': df.to_dict('records'),
                        'dataframe': df,
                        'total': stats.get('total_records', 0)
                    }
                    break  # 只处理第一个分组统计
        
        # 如果没有 grouped_stats，使用 basic_stats
        if chart_data is None and 'basic_stats' in stats:
            bs = stats['basic_stats']
            
            if 'by_operator' in bs and analysis_type == "按运营商统计":
                operator_df = pd.DataFrame(
                    list(bs['by_operator'].items()),
                    columns=['运营商', '数量']
                )
                operator_df = operator_df.sort_values('数量', ascending=False)
                
                chart_data = {
                    'type': '运营商分布',
                    'data': operator_df.to_dict('records'),
                    'dataframe': operator_df,
                    'total': stats.get('total_records', 0)
                }
            
            elif analysis_type == "按区域统计":
                # 根据 region_filter 智能选择下一级统计
                location_key, location_label = self._determine_region_chart_level(region_filter, bs)
                
                if location_key and location_key in bs and bs[location_key]:
                    location_df = pd.DataFrame(
                        list(bs[location_key].items()),
                        columns=['区域', '数量']
                    )
                    location_df = location_df.sort_values('数量', ascending=False).head(20)
                    
                    chart_data = {
                        'type': location_label,
                        'data': location_df.to_dict('records'),
                        'dataframe': location_df,
                        'total': stats.get('total_records', 0)
                    }
            
            elif 'by_type' in bs and analysis_type == "按类型统计":
                type_df = pd.DataFrame(
                    list(bs['by_type'].items()),
                    columns=['类型', '数量']
                )
                type_df = type_df.sort_values('数量', ascending=False)
                
                chart_data = {
                    'type': '类型分布',
                    'data': type_df.to_dict('records'),
                    'dataframe': type_df,
                    'total': stats.get('total_records', 0)
                }
            
            elif 'by_station' in bs and analysis_type == "按充电站统计":
                station_df = pd.DataFrame(
                    list(bs['by_station'].items()),
                    columns=['充电站编号', '数量']
                )
                station_df = station_df.sort_values('数量', ascending=False).head(50)  # 限制显示前50个充电站
                
                chart_data = {
                    'type': '充电站分布',
                    'data': station_df.to_dict('records'),
                    'dataframe': station_df,
                    'total': stats.get('total_records', 0)
                }
        
        return chart_data
    
    def _determine_region_chart_level(self, region_filter: Optional[Dict[str, str]], 
                                      basic_stats: Dict) -> tuple:
        """
        根据区域筛选条件智能确定图表应该显示的统计级别
        
        逻辑：
        - 无筛选 → 显示省份分布
        - 选择省份 → 显示该省下的城市分布
        - 选择城市/直辖市 → 显示区县分布
        - 选择区县 → 显示区县分布（当前级别）
        
        :param region_filter: 区域筛选条件
        :param basic_stats: 基础统计数据
        :return: (location_key, location_label) 元组
        """
        # 直辖市列表
        direct_cities = ['北京市', '上海市', '天津市', '重庆市', '北京', '上海', '天津', '重庆']
        
        if region_filter:
            province = region_filter.get('province', '')
            city = region_filter.get('city', '')
            district = region_filter.get('district', '')
            
            # 直辖市特殊处理：选择直辖市时直接显示区县
            if province in direct_cities or city in direct_cities:
                if 'by_location' in basic_stats and basic_stats['by_location']:
                    return 'by_location', '区县分布'
            
            # 已选择区县 → 显示区县分布
            if district:
                if 'by_location' in basic_stats and basic_stats['by_location']:
                    return 'by_location', '区县分布'
            
            # 已选择城市 → 显示区县分布（下钻到下一级）
            elif city:
                if 'by_location' in basic_stats and basic_stats['by_location']:
                    return 'by_location', '区县分布'
            
            # 已选择省份 → 显示城市分布（下钻到下一级）
            elif province:
                if 'by_city' in basic_stats and basic_stats['by_city']:
                    return 'by_city', '城市分布'
                # 回退：如果没有城市数据，尝试显示区县
                elif 'by_location' in basic_stats and basic_stats['by_location']:
                    return 'by_location', '区县分布'
        
        # 无筛选 → 显示省份分布
        if 'by_province' in basic_stats and basic_stats['by_province']:
            return 'by_province', '省份分布'
        # 回退：如果没有省份数据，尝试城市
        elif 'by_city' in basic_stats and basic_stats['by_city']:
            return 'by_city', '城市分布'
        # 最后回退：区县
        elif 'by_location' in basic_stats and basic_stats['by_location']:
            return 'by_location', '区县分布'
        
        return None, "区域分布"
    
    def create_bar_chart(self, df: pd.DataFrame, x_col: str, y_col: str, title: str):
        """
        创建柱状图
        :param df: 数据框
        :param x_col: X轴列名
        :param y_col: Y轴列名
        :param title: 图表标题
        :return: None (直接显示图表)
        """
        # 检查数据框是否为空
        if df is None or df.empty:
            st.info("📊 暂无数据可供展示")
            return
        
        # 检查必需的列是否存在
        if x_col not in df.columns or y_col not in df.columns:
            st.warning(f"⚠️ 数据框缺少必需的列: {x_col} 或 {y_col}")
            return
        
        # 过滤掉空值，避免 Altair 类型推断警告
        df_clean = df.dropna(subset=[x_col, y_col])
        
        if df_clean.empty:
            st.info("📊 数据全部为空，无法生成图表")
            return
        
        # 确保数值列为数值类型
        if df_clean[y_col].dtype == 'object':
            df_clean[y_col] = pd.to_numeric(df_clean[y_col], errors='coerce')
            df_clean = df_clean.dropna(subset=[y_col])
        
        if df_clean.empty:
            st.info("📊 数值列数据无效，无法生成图表")
            return
        
        # 创建图表
        try:
            st.bar_chart(df_clean.set_index(x_col))
        except Exception as e:
            st.error(f"❌ 生成柱状图失败: {str(e)}")
    
    def create_pie_chart(self, df: pd.DataFrame, values_col: str, names_col: str, title: str):
        """
        创建饼图
        :param df: 数据框
        :param values_col: 数值列名
        :param names_col: 名称列名
        :param title: 图表标题
        :return: Plotly图表对象
        """
        # 检查数据框是否为空
        if df is None or df.empty:
            return None
        
        # 检查必需的列是否存在
        if values_col not in df.columns or names_col not in df.columns:
            return None
        
        # 过滤掉空值
        df_clean = df.dropna(subset=[values_col, names_col])
        
        if df_clean.empty:
            return None
        
        # 确保数值列为数值类型
        if df_clean[values_col].dtype == 'object':
            df_clean[values_col] = pd.to_numeric(df_clean[values_col], errors='coerce')
            df_clean = df_clean.dropna(subset=[values_col])
        
        if df_clean.empty:
            return None
        
        # 创建图表
        try:
            fig = px.pie(
                df_clean,
                values=values_col,
                names=names_col,
                title=title
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
        except Exception as e:
            return None
    
    def prepare_power_statistics(self, stats: Dict) -> Optional[Dict]:
        """
        准备功率统计数据用于展示
        :param stats: 统计数据字典
        :return: 功率统计展示数据字典，如果没有功率数据则返回None
        """
        if 'basic_stats' not in stats:
            return None
        
        power_stats = stats['basic_stats'].get('by_power', {})
        if not power_stats or power_stats.get('total_count', 0) == 0:
            return None
        
        # 构建功率统计概览DataFrame（数值取整，不带单位）
        power_data = {
            '指标': ['有效数据量', '平均功率(kW)', '中位数功率(kW)', '最小功率(kW)', '最大功率(kW)', '标准差(kW)'],
            '数值': [
                int(power_stats.get('total_count', 0)),
                int(round(power_stats.get('mean', 0))),
                int(round(power_stats.get('median', 0))),
                int(round(power_stats.get('min', 0))),
                int(round(power_stats.get('max', 0))),
                int(round(power_stats.get('std', 0)))
            ]
        }
        
        # 功率区间分布
        by_range = power_stats.get('by_range', {})
        if by_range:
            range_order = ['≤7kW（慢充）', '7-30kW（小功率）', '30-60kW（中功率）', '60-120kW（大功率）', '>120kW（超快充）']
            range_data = []
            total_count = power_stats.get('total_count', 1)
            for label in range_order:
                if label in by_range:
                    count = by_range[label]
                    percentage = (count / total_count) * 100 if total_count > 0 else 0
                    range_data.append({
                        '功率区间': label,
                        '数量': count,
                        '占比': f"{percentage:.2f}%"
                    })
            range_df = pd.DataFrame(range_data)
        else:
            range_df = pd.DataFrame()
        
        return {
            'summary': pd.DataFrame(power_data),
            'by_range': range_df,
            'total_count': power_stats.get('total_count', 0)
        }

