# handlers/chart_generator.py - 图表生成器

import pandas as pd
import plotly.express as px
from typing import Dict, Optional


class ChartGenerator:
    """图表生成器，用于生成研报中的统计图表"""
    
    @staticmethod
    def create_operator_charts(operator_stats: Dict, total_records: int) -> Optional[Dict]:
        """创建运营商图表数据"""
        try:
            sorted_operators = sorted(operator_stats.items(), key=lambda x: x[1], reverse=True)
            top_10 = sorted_operators[:10]  # 只显示前10名
            
            df = pd.DataFrame(top_10, columns=['运营商', '数量'])
            df['占比'] = (df['数量'] / total_records * 100).round(2)
            
            # 创建柱状图
            fig_bar = px.bar(
                df,
                x='运营商',
                y='数量',
                title='主要运营商排名（Top 10）',
                labels={'数量': '充电桩数量（台）', '运营商': '运营商名称'}
            )
            fig_bar.update_layout(xaxis_tickangle=-45, height=400)
            
            # 创建饼图
            fig_pie = px.pie(
                df,
                values='数量',
                names='运营商',
                title='运营商市场份额分布（Top 10）'
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            
            return {
                'bar_chart': fig_bar,
                'pie_chart': fig_pie,
                'dataframe': df
            }
        except Exception as e:
            return None
    
    @staticmethod
    def create_location_charts(location_stats: Dict, total_records: int) -> Optional[Dict]:
        """创建区域分布图表数据"""
        try:
            sorted_locations = sorted(location_stats.items(), key=lambda x: x[1], reverse=True)
            top_15 = sorted_locations[:15]  # 只显示前15名
            
            df = pd.DataFrame(top_15, columns=['区域', '数量'])
            df['占比'] = (df['数量'] / total_records * 100).round(2)
            
            # 创建柱状图
            fig_bar = px.bar(
                df,
                x='区域',
                y='数量',
                title='主要区域分布（Top 15）',
                labels={'数量': '充电桩数量（台）', '区域': '区域名称'}
            )
            fig_bar.update_layout(xaxis_tickangle=-45, height=400)
            
            # 创建饼图
            fig_pie = px.pie(
                df,
                values='数量',
                names='区域',
                title='区域分布占比（Top 15）'
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            
            return {
                'bar_chart': fig_bar,
                'pie_chart': fig_pie,
                'dataframe': df
            }
        except Exception as e:
            return None
    
    @staticmethod
    def create_type_charts(type_stats: Dict, total_records: int) -> Optional[Dict]:
        """创建充电桩类型图表数据"""
        try:
            sorted_types = sorted(type_stats.items(), key=lambda x: x[1], reverse=True)
            
            df = pd.DataFrame(sorted_types, columns=['类型', '数量'])
            df['占比'] = (df['数量'] / total_records * 100).round(2)
            
            # 创建柱状图
            fig_bar = px.bar(
                df,
                x='类型',
                y='数量',
                title='充电桩类型分布',
                labels={'数量': '充电桩数量（台）', '类型': '充电桩类型'}
            )
            fig_bar.update_layout(height=400)
            
            # 创建饼图
            fig_pie = px.pie(
                df,
                values='数量',
                names='类型',
                title='充电桩类型占比分布'
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            
            return {
                'bar_chart': fig_bar,
                'pie_chart': fig_pie,
                'dataframe': df
            }
        except Exception as e:
            return None
    
    @staticmethod
    def create_power_charts(power_stats: Dict, total_records: int) -> Optional[Dict]:
        """创建额定功率图表数据"""
        try:
            by_range = power_stats.get('by_range', {})
            if not by_range:
                return None
            
            # 按定义的顺序排序
            range_order = ['≤7kW（慢充）', '7-30kW（小功率）', '30-60kW（中功率）', '60-120kW（大功率）', '>120kW（超快充）']
            sorted_ranges = []
            for label in range_order:
                if label in by_range:
                    sorted_ranges.append((label, by_range[label]))
            
            if not sorted_ranges:
                return None
            
            df = pd.DataFrame(sorted_ranges, columns=['功率区间', '数量'])
            total_count = power_stats.get('total_count', sum(df['数量']))
            df['占比'] = (df['数量'] / total_count * 100).round(2)
            
            # 创建柱状图
            fig_bar = px.bar(
                df,
                x='功率区间',
                y='数量',
                title='额定功率区间分布',
                labels={'数量': '充电桩数量（台）', '功率区间': '功率区间'}
            )
            fig_bar.update_layout(height=400)
            
            # 创建饼图
            fig_pie = px.pie(
                df,
                values='数量',
                names='功率区间',
                title='功率区间占比分布'
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            
            return {
                'bar_chart': fig_bar,
                'pie_chart': fig_pie,
                'dataframe': df
            }
        except Exception as e:
            return None

