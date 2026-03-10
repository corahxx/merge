# handlers/industry_report_handler.py - 行业研报生成处理

from typing import Dict, Optional
from datetime import datetime, date
import pandas as pd
import plotly.express as px
from data.data_processor import DataProcessor
from data.error_handler import ErrorHandler
from core.condition_parser import ConditionParser
from handlers.chart_generator import ChartGenerator


class IndustryReportHandler:
    """处理行业研报生成逻辑"""
    
    def __init__(self, table_name: str, llm=None):
        """
        初始化行业研报处理器
        :param table_name: 数据表名
        :param llm: 可选的大语言模型实例（用于AI生成结论）
        """
        self.table_name = table_name
        self.processor = DataProcessor(table_name=table_name, verbose=False)
        self.condition_parser = ConditionParser(table_name=table_name)
        self.llm = llm
    
    def generate_report(self, 
                       question: str = "",
                       limit: Optional[int] = None,
                       date_field: Optional[str] = None,
                       start_date: Optional[date] = None,
                       end_date: Optional[date] = None,
                       operator_filter: Optional[list] = None) -> Dict:
        """
        生成行业研报
        :param question: 用户问题，用于定制研报内容
        :param limit: 统计记录数限制
        :param date_field: 日期字段名
        :param start_date: 起始日期
        :param end_date: 结束日期
        :param operator_filter: 运营商筛选列表
        :return: 研报字典
        """
        try:
            # 第一步：解析用户问题中的条件
            parsed_conditions = self.condition_parser.parse_conditions(question)
            
            # 使用解析到的条件，如果没有明确提供参数的话
            if not date_field and parsed_conditions.get('date_field'):
                date_field = parsed_conditions['date_field']
            if not start_date and parsed_conditions.get('start_date'):
                start_date = parsed_conditions['start_date']
            if not end_date and parsed_conditions.get('end_date'):
                end_date = parsed_conditions['end_date']
            if not operator_filter and parsed_conditions.get('operator_filter'):
                operator_filter = parsed_conditions['operator_filter']
            
            # 使用解析到的区域筛选条件（优先使用 region_filter）
            region_filter = parsed_conditions.get('region_filter')
            location_filter = parsed_conditions.get('location_filter')  # 保留兼容
            
            # 第二步：根据条件查询数据库
            stats = self.processor.get_database_statistics(
                group_by=None,
                limit=limit,
                date_field=date_field,
                start_date=start_date,
                end_date=end_date,
                operator_filter=operator_filter,
                location_filter=location_filter,  # 兼容旧接口
                region_filter=region_filter  # 使用新的三级区域筛选（精准检索）
            )
            
            if 'error' in stats:
                return {
                    'success': False,
                    'error': stats['error']
                }
            
            # 第三步：分析问题，确定研报重点
            focus_areas = self._analyze_question(question)
            
            # 第四步：生成研报内容（包含条件信息）
            report = self._build_report(
                stats, 
                question, 
                focus_areas, 
                date_field, 
                start_date, 
                end_date,
                parsed_conditions
            )
            
            return {
                'success': True,
                'report': report,
                'stats': stats
            }
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "生成行业研报")
            return {
                'success': False,
                'error': error_info.get('error_message', str(e)),
                'error_details': error_info
            }
    
    def _analyze_question(self, question: str) -> Dict[str, bool]:
        """
        分析问题，确定研报重点
        :param question: 用户问题
        :return: 重点领域字典
        """
        question_lower = question.lower()
        
        # 默认所有领域都包含（如果是完整报告）
        focus = {
            'operator': False,  # 运营商
            'location': False,  # 区域
            'type': False,      # 类型
            'power': False,     # 额定功率
            'overview': True,   # 概况（默认包含）
            'conclusion': True  # 结论（默认包含）
        }
        
        # 检查是否要求完整报告
        full_report_keywords = ['完整', '全面', '全部', '所有', '整体', '综合', '整个']
        is_full_report = any(kw in question for kw in full_report_keywords)
        
        if is_full_report:
            # 完整报告包含所有内容
            focus['operator'] = True
            focus['location'] = True
            focus['type'] = True
            focus['power'] = True
            return focus
        
        # 根据关键词确定重点
        operator_keywords = ['运营商', '运营', '企业', '公司', '品牌']
        location_keywords = ['区域', '地区', '城市', '省份', '分布', '布局', '位置', '地点']
        type_keywords = ['类型', '种类', '型号', '技术', '设备']
        power_keywords = ['功率', '快充', '慢充', '充电速度', '充电能力']
        
        # 检查运营商相关
        if any(kw in question for kw in operator_keywords):
            focus['operator'] = True
        
        # 检查区域相关
        if any(kw in question for kw in location_keywords):
            focus['location'] = True
        
        # 检查类型相关
        if any(kw in question for kw in type_keywords):
            focus['type'] = True
        
        # 检查功率相关
        if any(kw in question for kw in power_keywords):
            focus['power'] = True
        
        # 如果没有任何特定关键词，生成完整报告
        if not any([focus['operator'], focus['location'], focus['type'], focus['power']]):
            focus['operator'] = True
            focus['location'] = True
            focus['type'] = True
            focus['power'] = True
        
        return focus
    
    def _build_report(self, stats: Dict, 
                     question: str,
                     focus_areas: Dict[str, bool],
                     date_field: Optional[str] = None,
                     start_date: Optional[date] = None,
                     end_date: Optional[date] = None,
                     parsed_conditions: Dict = None) -> Dict:
        """构建研报内容"""
        # 根据问题确定标题
        title = self._generate_title(question)
        
        # 生成条件摘要
        conditions_summary = ""
        if parsed_conditions and parsed_conditions.get('parsed_conditions'):
            conditions_summary = "；".join(parsed_conditions['parsed_conditions'])
        
        report = {
            'title': title,
            'generation_date': datetime.now().strftime('%Y年%m月%d日'),
            'data_period': self._format_data_period(date_field, start_date, end_date),
            'conditions_summary': conditions_summary,
            'sections': []
        }
        
        # 如果有筛选条件，添加条件说明部分
        if conditions_summary:
            report['sections'].append({
                'title': '数据筛选条件',
                'content': f"本报告基于以下筛选条件生成：\n\n**{conditions_summary}**\n\n报告中的所有统计数据均基于上述筛选条件的结果。"
            })
        
        basic_stats = stats.get('basic_stats', {})
        total_records = stats.get('total_records', 0)
        
        # 1. 执行摘要（始终包含）
        report['sections'].append({
            'title': '执行摘要',
            'content': self._generate_executive_summary(basic_stats, total_records, question, focus_areas)
        })
        
        # 2. 市场概况（如果focus包含overview）
        if focus_areas.get('overview', True):
            report['sections'].append({
                'title': '市场概况',
                'content': self._generate_market_overview(basic_stats, total_records)
            })
        
        # 3. 运营商分析（如果focus包含operator）
        if focus_areas.get('operator', False) and 'by_operator' in basic_stats:
            operator_stats = basic_stats['by_operator']
            # 生成运营商图表数据
            chart_data = ChartGenerator.create_operator_charts(operator_stats, total_records)
            report['sections'].append({
                'title': '运营商市场分析',
                'content': self._generate_operator_analysis(operator_stats, total_records),
                'chart_data': chart_data
            })
        
        # 4. 区域分布分析（如果focus包含location）
        if focus_areas.get('location', False):
            # 根据查询颗粒度选择区域统计字段
            location_stats = None
            location_title = '区域分布分析'
            
            # 获取 region_filter 来判断查询级别
            region_filter = parsed_conditions.get('region_filter') if parsed_conditions else None
            
            # 判断应该使用哪个级别的统计
            if region_filter:
                province = region_filter.get('province', '')
                city = region_filter.get('city', '')
                district = region_filter.get('district', '')
                
                # 直辖市特殊处理：北京、上海、天津、重庆直接用区县分析
                direct_cities = ['北京市', '上海市', '天津市', '重庆市']
                if province in direct_cities or city in direct_cities:
                    # 直辖市 → 使用区县统计
                    if 'by_location' in basic_stats:
                        location_stats = basic_stats['by_location']
                        location_title = '区域分布分析（按区县）'
                elif district:
                    # 区县级别查询 → 使用区县统计
                    if 'by_location' in basic_stats:
                        location_stats = basic_stats['by_location']
                        location_title = '区域分布分析（按区县）'
                elif city:
                    # 城市级别查询 → 使用城市统计
                    if 'by_city' in basic_stats:
                        location_stats = basic_stats['by_city']
                        location_title = '区域分布分析（按城市）'
                    elif 'by_location' in basic_stats:
                        location_stats = basic_stats['by_location']
                        location_title = '区域分布分析（按区县）'
                elif province:
                    # 省份级别查询 → 使用城市统计
                    if 'by_city' in basic_stats:
                        location_stats = basic_stats['by_city']
                        location_title = '区域分布分析（按城市）'
                    elif 'by_location' in basic_stats:
                        location_stats = basic_stats['by_location']
                        location_title = '区域分布分析（按区县）'
            else:
                # 全量数据 → 使用省份统计
                if 'by_province' in basic_stats:
                    location_stats = basic_stats['by_province']
                    location_title = '区域分布分析（按省份）'
                elif 'by_city' in basic_stats:
                    location_stats = basic_stats['by_city']
                    location_title = '区域分布分析（按城市）'
                elif 'by_location' in basic_stats:
                    location_stats = basic_stats['by_location']
                    location_title = '区域分布分析（按区县）'
            
            if location_stats:
                # 生成区域分布图表数据
                chart_data = ChartGenerator.create_location_charts(location_stats, total_records)
                report['sections'].append({
                    'title': location_title,
                    'content': self._generate_location_analysis(location_stats, total_records),
                    'chart_data': chart_data
                })
        
        # 5. 类型分析（如果focus包含type）
        if focus_areas.get('type', False) and 'by_type' in basic_stats:
            type_stats = basic_stats['by_type']
            # 生成类型图表数据
            chart_data = ChartGenerator.create_type_charts(type_stats, total_records)
            report['sections'].append({
                'title': '充电桩类型分析',
                'content': self._generate_type_analysis(type_stats, total_records),
                'chart_data': chart_data
            })
        
        # 6. 额定功率分析（如果focus包含power）
        if focus_areas.get('power', False) and 'by_power' in basic_stats:
            power_stats = basic_stats['by_power']
            # 生成功率图表数据
            chart_data = ChartGenerator.create_power_charts(power_stats, total_records)
            report['sections'].append({
                'title': '额定功率分析',
                'content': self._generate_power_analysis(power_stats, total_records),
                'chart_data': chart_data
            })
        
        # 7. 结论与建议（始终包含，标记为需要AI流式生成）
        if focus_areas.get('conclusion', True):
            # 标记为需要AI生成，实际生成将在format_report_to_text时进行流式输出
            report['sections'].append({
                'title': '结论与建议',
                'content': None,  # 标记为None，表示需要AI生成
                'needs_ai_generation': True,
                'fallback_content': self._generate_conclusions(basic_stats, total_records, focus_areas)  # 后备内容
            })
        
        return report
    
    def _generate_title(self, question: str) -> str:
        """根据问题生成标题"""
        question_lower = question.lower()
        
        if '运营商' in question or '运营' in question:
            return '充电桩运营商市场分析报告'
        elif '区域' in question or '地区' in question or '城市' in question:
            return '充电桩区域分布分析报告'
        elif '类型' in question or '技术' in question:
            return '充电桩类型分析报告'
        elif '功率' in question or '快充' in question or '慢充' in question:
            return '充电桩额定功率分析报告'
        else:
            return '充电桩行业数据研究报告'
    
    def _format_data_period(self, date_field: Optional[str], 
                           start_date: Optional[date], 
                           end_date: Optional[date]) -> str:
        """格式化数据时间范围"""
        if date_field and (start_date or end_date):
            start_str = start_date.strftime('%Y年%m月%d日') if start_date else '最早'
            end_str = end_date.strftime('%Y年%m月%d日') if end_date else '最新'
            return f"{start_str} 至 {end_str}"
        return "全量数据"
    
    def _generate_executive_summary(self, basic_stats: Dict, total_records: int, 
                                   question: str, focus_areas: Dict[str, bool]) -> str:
        """生成执行摘要"""
        unique_piles = basic_stats.get('unique_piles', 0)
        unique_stations = basic_stats.get('unique_stations', 0)
        
        # 根据focus_areas确定分析维度
        analysis_dims = []
        if focus_areas.get('operator', False):
            analysis_dims.append('运营商分布')
        if focus_areas.get('location', False):
            analysis_dims.append('区域布局')
        if focus_areas.get('type', False):
            analysis_dims.append('设备类型')
        if focus_areas.get('power', False):
            analysis_dims.append('额定功率')
        
        dims_text = '、'.join(analysis_dims) if analysis_dims else '多个维度'
        
        # 计算平均每站充电桩数
        avg_piles_per_station = (unique_piles / unique_stations if unique_stations > 0 else 0)
        
        summary = f"""
本报告基于数据库真实数据分析，针对用户问题进行了定制化分析。

**核心数据：**
- 总记录数：{total_records:,} 条
- 充电站数量：{unique_stations:,} 座
- 充电桩数量：{unique_piles:,} 台
- 平均每站充电桩数：{avg_piles_per_station:.1f} 台/站

本报告重点从{dims_text}等维度进行了深入分析，为行业决策提供数据支撑。
        """.strip()
        
        return summary
    
    def _generate_market_overview(self, basic_stats: Dict, total_records: int) -> str:
        """生成市场概况"""
        unique_piles = basic_stats.get('unique_piles', 0)
        unique_stations = basic_stats.get('unique_stations', 0)
        operator_count = len(basic_stats.get('by_operator', {}))
        location_count = len(basic_stats.get('by_location', {}))
        type_count = len(basic_stats.get('by_type', {}))
        
        # 计算平均每站充电桩数
        avg_piles_per_station = (unique_piles / unique_stations if unique_stations > 0 else 0)
        
        overview = f"""
**市场总体规模**

根据数据库统计，当前市场呈现出以下特征：

- **充电站总量**：{unique_stations:,} 座（唯一充电站数量）
- **充电桩总量**：{unique_piles:,} 台（唯一充电桩数量）
- **平均每站充电桩数**：{avg_piles_per_station:.1f} 台/站
- **运营商数量**：{operator_count} 家
- **覆盖区域**：{location_count} 个区县
- **设备类型**：{type_count} 种

市场呈现出多运营商竞争、区域分布广泛、设备类型多样化的特点。充电站和充电桩的规模反映了行业的整体发展水平。
        """.strip()
        
        return overview
    
    def _generate_operator_analysis(self, operator_stats: Dict, total_records: int) -> str:
        """生成运营商分析"""
        # 按数量排序
        sorted_operators = sorted(operator_stats.items(), key=lambda x: x[1], reverse=True)
        top_10 = sorted_operators[:10]
        
        analysis = "**主要运营商排名（Top 10）**\n\n"
        
        for i, (operator, count) in enumerate(top_10, 1):
            percentage = (count / total_records * 100) if total_records > 0 else 0
            analysis += f"{i}. **{operator}**：{count:,} 台（占比 {percentage:.2f}%）\n"
        
        # 市场集中度分析
        if len(sorted_operators) > 0:
            top_3_count = sum(count for _, count in sorted_operators[:3])
            top_5_count = sum(count for _, count in sorted_operators[:5])
            cr3 = (top_3_count / total_records * 100) if total_records > 0 else 0
            cr5 = (top_5_count / total_records * 100) if total_records > 0 else 0
            
            analysis += f"\n**市场集中度分析**\n"
            analysis += f"- CR3（前三名市场份额）：{cr3:.2f}%\n"
            analysis += f"- CR5（前五名市场份额）：{cr5:.2f}%\n"
            
            if cr3 > 50:
                analysis += "\n市场集中度较高，头部运营商占据主导地位。\n"
            elif cr5 > 60:
                analysis += "\n市场集中度中等，前五大运营商占据主要市场份额。\n"
            else:
                analysis += "\n市场集中度较低，呈现分散竞争格局。\n"
        
        return analysis.strip()
    
    def _generate_location_analysis(self, location_stats: Dict, total_records: int) -> str:
        """生成区域分析"""
        sorted_locations = sorted(location_stats.items(), key=lambda x: x[1], reverse=True)
        top_15 = sorted_locations[:15]
        
        analysis = "**主要区域分布（Top 15）**\n\n"
        
        for i, (location, count) in enumerate(top_15, 1):
            percentage = (count / total_records * 100) if total_records > 0 else 0
            analysis += f"{i}. **{location}**：{count:,} 台（占比 {percentage:.2f}%）\n"
        
        # 区域集中度
        if len(sorted_locations) > 0:
            top_5_count = sum(count for _, count in sorted_locations[:5])
            top_10_count = sum(count for _, count in sorted_locations[:10])
            concentration_5 = (top_5_count / total_records * 100) if total_records > 0 else 0
            concentration_10 = (top_10_count / total_records * 100) if total_records > 0 else 0
            
            analysis += f"\n**区域集中度**\n"
            analysis += f"- 前5大区域占比：{concentration_5:.2f}%\n"
            analysis += f"- 前10大区域占比：{concentration_10:.2f}%\n"
            
            if concentration_5 > 40:
                analysis += "\n区域分布较为集中，主要集中在一线及重点城市。\n"
            else:
                analysis += "\n区域分布相对分散，覆盖范围较广。\n"
        
        return analysis.strip()
    
    def _generate_type_analysis(self, type_stats: Dict, total_records: int) -> str:
        """生成类型分析"""
        sorted_types = sorted(type_stats.items(), key=lambda x: x[1], reverse=True)
        
        analysis = "**充电桩类型分布**\n\n"
        
        for type_name, count in sorted_types:
            percentage = (count / total_records * 100) if total_records > 0 else 0
            analysis += f"- **{type_name}**：{count:,} 台（占比 {percentage:.2f}%）\n"
        
        # 类型特征分析
        if len(sorted_types) > 0:
            dominant_type, dominant_count = sorted_types[0]
            dominant_percentage = (dominant_count / total_records * 100) if total_records > 0 else 0
            
            analysis += f"\n**类型特征**\n"
            analysis += f"- 主流类型：{dominant_type}（占比 {dominant_percentage:.2f}%）\n"
            
            if dominant_percentage > 60:
                analysis += f"\n市场以{dominant_type}为主，类型集中度较高。\n"
            else:
                analysis += f"\n市场类型多样化，{dominant_type}占据主导地位。\n"
        
        return analysis.strip()
    
    def _generate_power_analysis(self, power_stats: Dict, total_records: int) -> str:
        """生成额定功率分析"""
        analysis = "**额定功率统计概览**\n\n"
        
        total_count = power_stats.get('total_count', 0)
        if total_count == 0:
            return "**额定功率分析**\n\n暂无额定功率数据。\n"
        
        mean_power = power_stats.get('mean', 0)
        median_power = power_stats.get('median', 0)
        min_power = power_stats.get('min', 0)
        max_power = power_stats.get('max', 0)
        
        analysis += f"- **有效数据量**：{total_count:,} 台（有额定功率记录）\n"
        analysis += f"- **平均功率**：{mean_power:.1f} kW\n"
        analysis += f"- **中位数功率**：{median_power:.1f} kW\n"
        analysis += f"- **功率范围**：{min_power:.1f} kW ~ {max_power:.1f} kW\n"
        
        # 按功率区间分布
        by_range = power_stats.get('by_range', {})
        if by_range:
            analysis += "\n**功率区间分布**\n\n"
            # 按定义的顺序排序
            range_order = ['≤7kW（慢充）', '7-30kW（小功率）', '30-60kW（中功率）', '60-120kW（大功率）', '>120kW（超快充）']
            sorted_ranges = []
            for label in range_order:
                if label in by_range:
                    sorted_ranges.append((label, by_range[label]))
            
            for label, count in sorted_ranges:
                percentage = (count / total_count * 100) if total_count > 0 else 0
                analysis += f"- **{label}**：{count:,} 台（占比 {percentage:.2f}%）\n"
            
            # 功率结构分析
            analysis += "\n**功率结构特征**\n"
            slow_charge = by_range.get('≤7kW（慢充）', 0)
            fast_charge = sum(by_range.get(label, 0) for label in ['7-30kW（小功率）', '30-60kW（中功率）', '60-120kW（大功率）', '>120kW（超快充）'])
            
            if slow_charge > fast_charge:
                analysis += "市场以慢充桩为主，适合长时间停车场景（如居民区、办公区）。\n"
            elif fast_charge > slow_charge * 2:
                analysis += "市场以快充桩为主，注重充电效率，适合快速补电场景（如高速公路、商业区）。\n"
            else:
                analysis += "慢充和快充桩分布相对均衡，市场结构较为合理。\n"
            
            # 识别主流功率
            if by_range:
                max_range_label = max(by_range.items(), key=lambda x: x[1])[0]
                max_range_count = by_range[max_range_label]
                max_range_percentage = (max_range_count / total_count * 100) if total_count > 0 else 0
                analysis += f"\n主流功率区间为**{max_range_label}**，占比 {max_range_percentage:.2f}%，反映了市场的主要需求导向。\n"
        
        return analysis.strip()
    
    def _generate_conclusions(self, basic_stats: Dict, total_records: int, 
                            focus_areas: Dict[str, bool] = None) -> str:
        """生成结论与建议"""
        if focus_areas is None:
            focus_areas = {'operator': True, 'location': True, 'type': True, 'power': True}
        
        unique_piles = basic_stats.get('unique_piles', 0)
        unique_stations = basic_stats.get('unique_stations', 0)
        conclusion_num = 1
        conclusions = "**核心发现**\n\n"
        
        # 市场规模结论（始终包含，同时提及充电站和充电桩）
        avg_piles_per_station = (unique_piles / unique_stations if unique_stations > 0 else 0)
        if unique_piles > 100000 and unique_stations > 10000:
            conclusions += f"{conclusion_num}. **市场规模**：行业规模庞大，充电站总量 {unique_stations:,} 座，充电桩总量超过 {unique_piles:,} 台（平均每站 {avg_piles_per_station:.1f} 台），显示出强劲的市场需求和发展潜力。\n\n"
        elif unique_piles > 10000 and unique_stations > 1000:
            conclusions += f"{conclusion_num}. **市场规模**：行业处于快速发展期，充电站总量 {unique_stations:,} 座，充电桩总量达 {unique_piles:,} 台（平均每站 {avg_piles_per_station:.1f} 台），市场增长空间较大。\n\n"
        else:
            conclusions += f"{conclusion_num}. **市场规模**：行业处于发展初期，充电站总量 {unique_stations:,} 座，充电桩总量为 {unique_piles:,} 台（平均每站 {avg_piles_per_station:.1f} 台），具有较大的增长潜力。\n\n"
        conclusion_num += 1
        
        # 竞争格局结论（如果focus包含operator）
        if focus_areas.get('operator', False):
            operator_stats = basic_stats.get('by_operator', {})
            sorted_operators = sorted(operator_stats.items(), key=lambda x: x[1], reverse=True)
            operator_count = len(operator_stats)
            
            if len(sorted_operators) > 0:
                top_operator, top_count = sorted_operators[0]
                top_percentage = (top_count / total_records * 100) if total_records > 0 else 0
                
                if top_percentage > 30:
                    conclusions += f"{conclusion_num}. **竞争格局**：市场呈现寡头竞争态势，{top_operator}占据领先地位（{top_percentage:.2f}%），头部效应明显。\n\n"
                elif operator_count > 20:
                    conclusions += f"{conclusion_num}. **竞争格局**：市场参与者众多（{operator_count}家运营商），竞争较为激烈，{top_operator}暂居首位。\n\n"
                else:
                    conclusions += f"{conclusion_num}. **竞争格局**：市场集中度适中，{operator_count}家运营商共同参与，{top_operator}占据优势。\n\n"
                conclusion_num += 1
        
        # 区域分布结论（如果focus包含location）
        if focus_areas.get('location', False):
            location_stats = basic_stats.get('by_location', {})
            sorted_locations = sorted(location_stats.items(), key=lambda x: x[1], reverse=True)
            
            if len(sorted_locations) > 0:
                top_location, top_location_count = sorted_locations[0]
                conclusions += f"{conclusion_num}. **区域分布**：{top_location}充电桩数量最多（{top_location_count:,} 台），区域发展不平衡，需要加强中西部和三四线城市的充电站和充电桩布局。\n\n"
                conclusion_num += 1
        
        # 类型分布结论（如果focus包含type）
        if focus_areas.get('type', False):
            type_stats = basic_stats.get('by_type', {})
            
            if len(type_stats) > 0:
                sorted_types = sorted(type_stats.items(), key=lambda x: x[1], reverse=True)
                dominant_type = sorted_types[0][0]
                conclusions += f"{conclusion_num}. **技术类型**：{dominant_type}为主流技术路线，技术发展相对集中。\n\n"
                conclusion_num += 1
        
        # 功率分布结论（如果focus包含power）
        if focus_areas.get('power', False):
            power_stats = basic_stats.get('by_power', {})
            
            if power_stats and power_stats.get('total_count', 0) > 0:
                mean_power = power_stats.get('mean', 0)
                by_range = power_stats.get('by_range', {})
                if by_range:
                    max_range_label = max(by_range.items(), key=lambda x: x[1])[0]
                    conclusions += f"{conclusion_num}. **功率特征**：平均额定功率为 {mean_power:.1f} kW，{max_range_label}为主流功率区间。\n\n"
                    conclusion_num += 1
        
        # 根据focus_areas生成相应的发展建议
        conclusions += "**发展建议**\n\n"
        suggestion_num = 1
        
        if focus_areas.get('location', False):
            conclusions += f"{suggestion_num}. **市场拓展**：建议运营商重点关注充电桩密度较低的区域，挖掘市场潜力。\n\n"
            suggestion_num += 1
        
        if focus_areas.get('type', False):
            conclusions += f"{suggestion_num}. **技术升级**：关注新技术趋势，适时进行设备更新和技术迭代。\n\n"
            suggestion_num += 1
        
        if focus_areas.get('operator', False):
            conclusions += f"{suggestion_num}. **运营优化**：通过数据驱动优化运营策略，提升设备利用率和用户满意度。\n\n"
            suggestion_num += 1
        
        # 通用建议
        if suggestion_num == 1:  # 如果没有特定建议，添加通用建议
            conclusions += "1. **市场拓展**：建议运营商重点关注充电桩密度较低的区域，挖掘市场潜力。\n\n"
            conclusions += "2. **技术升级**：关注新技术趋势，适时进行设备更新和技术迭代。\n\n"
            conclusions += "3. **运营优化**：通过数据驱动优化运营策略，提升设备利用率和用户满意度。\n"
        else:
            conclusions += f"{suggestion_num}. **合作共赢**：鼓励运营商之间加强合作，推动行业标准化和互联互通。\n"
        
        return conclusions.strip()
    
    def generate_ai_conclusions(self, report: Dict, stats: Dict, target_length: int = 800) -> str:
        """
        使用LLM生成AI结论与建议
        :param report: 报告字典（包含所有章节内容）
        :param stats: 统计数据字典
        :param target_length: 目标字数（默认800字）
        :return: AI生成的结论与建议文本
        """
        if not self.llm:
            return "❌ LLM未初始化，无法生成AI结论。"
        
        try:
            # 提取报告的关键数据
            basic_stats = stats.get('basic_stats', {})
            total_records = stats.get('total_records', 0)
            
            # 构建数据摘要
            data_summary = []
            data_summary.append(f"总记录数: {total_records:,} 条")
            
            unique_piles = basic_stats.get('unique_piles', 0)
            unique_stations = basic_stats.get('unique_stations', 0)
            if unique_piles > 0:
                data_summary.append(f"充电桩数量: {unique_piles:,} 台")
            if unique_stations > 0:
                data_summary.append(f"充电站数量: {unique_stations:,} 座")
            
            # 提取各章节的关键信息
            sections_text = []
            for section in report.get('sections', []):
                title = section.get('title', '')
                content = section.get('content', '')
                if title and content:
                    sections_text.append(f"## {title}\n{content}")
            
            # 构建提示词
            prompt = f"""你是一位充电桩行业数据分析专家。请基于以下数据统计结果，生成一份专业的结论与建议报告。

**数据概览：**
{chr(10).join(data_summary)}

**报告内容：**
{chr(10).join(sections_text)}

**要求：**
1. 生成约{target_length}字的专业分析报告
2. 包含"核心发现"和"发展建议"两个部分
3. 核心发现应基于数据提出3-5个关键洞察
4. 发展建议应具有可操作性，针对行业发展趋势提出3-5条建议
5. 使用Markdown格式，使用**加粗**突出重要内容
6. 语言专业、客观、有深度
7. 避免重复报告中的数据，重点在于分析和洞察

请开始生成："""

            # 调用LLM生成
            response = self.llm.invoke(prompt)
            
            # 提取文本内容（LLM可能返回不同格式）
            if isinstance(response, str):
                ai_conclusions = response
            elif hasattr(response, 'content'):
                ai_conclusions = response.content
            else:
                ai_conclusions = str(response)
            
            return ai_conclusions.strip()
            
        except Exception as e:
            error_msg = f"生成AI结论时发生错误: {str(e)}"
            return f"❌ {error_msg}"
    
    def generate_ai_conclusions_stream(self, report: Dict, stats: Dict, target_length: int = 1200):
        """
        使用LLM流式生成AI结论与建议
        :param report: 报告字典（包含所有章节内容）
        :param stats: 统计数据字典
        :param target_length: 目标字数（默认1200字）
        :return: 生成器，yield每个token
        """
        if not self.llm:
            yield "❌ LLM未初始化，无法生成AI结论。"
            return
        
        try:
            # 提取报告的关键数据
            basic_stats = stats.get('basic_stats', {})
            total_records = stats.get('total_records', 0)
            
            # 构建数据摘要
            data_summary = []
            data_summary.append(f"总记录数: {total_records:,} 条")
            
            unique_piles = basic_stats.get('unique_piles', 0)
            unique_stations = basic_stats.get('unique_stations', 0)
            if unique_piles > 0:
                data_summary.append(f"充电桩数量: {unique_piles:,} 台")
            if unique_stations > 0:
                data_summary.append(f"充电站数量: {unique_stations:,} 座")
            
            # 计算关键指标
            avg_piles = unique_piles / unique_stations if unique_stations > 0 else 0
            if avg_piles > 0:
                data_summary.append(f"平均每站充电桩数: {avg_piles:.1f} 台")
            
            # 提取运营商集中度
            operator_stats = basic_stats.get('by_operator', {})
            if operator_stats:
                sorted_ops = sorted(operator_stats.items(), key=lambda x: x[1], reverse=True)
                top3_count = sum(c for _, c in sorted_ops[:3])
                cr3 = (top3_count / total_records * 100) if total_records > 0 else 0
                data_summary.append(f"CR3（前三运营商市场份额）: {cr3:.1f}%")
            
            # 提取各章节的关键信息
            sections_text = []
            for section in report.get('sections', []):
                title = section.get('title', '')
                content = section.get('content', '')
                if title and content:
                    sections_text.append(f"## {title}\n{content}")
            
            # 构建增强版提示词
            prompt = f"""你是一位资深的电动汽车充电基础设施行业分析师，正在撰写行业研究报告的结论与建议部分。

## 【角色定位】
- 具备10年以上行业研究经验
- 深谙充电桩行业的竞争格局、技术路线和政策走向
- 善于从数据中提炼战略洞察和投资建议

## 【数据背景】
{chr(10).join(data_summary)}

## 【已完成的报告章节】
{chr(10).join(sections_text)}

## 【撰写任务】
请基于以上数据和报告内容，撰写一份约{target_length}字的深度结论与建议章节。

## 【内容要求】

### 核心发现（约400字）
- 从市场规模、竞争格局、区域分布、技术结构四个维度提炼4-6条核心洞察
- 每条洞察必须有数据支撑，并揭示其背后的行业含义
- 避免简单复述数据，要提供分析性观点

### 战略研判（约400字）
- 分析当前市场所处的发展阶段和演进方向
- 判断行业竞争格局的未来走势
- 识别数据反映出的结构性机会和挑战

### 发展建议（约400字）
分别针对以下三类主体提供可操作建议：
1. **运营商**：市场拓展、设备布局、服务优化方向
2. **投资者**：投资机会、风险点、关注领域
3. **政策制定者**：基础设施规划、市场监管建议

## 【格式要求】
- 使用Markdown格式
- 使用**加粗**突出关键数据和核心观点
- 结构清晰，使用小标题分隔内容
- 语言专业、客观、具有前瞻性
- **禁止输出任何思考过程或英文内容**

---

请直接开始输出结论与建议：

**核心发现**

"""

            # 使用流式输出
            started_output = False
            consecutive_english_count = 0
            
            for chunk in self.llm.stream(prompt):
                # 提取文本内容
                if isinstance(chunk, str):
                    chunk_text = chunk
                elif hasattr(chunk, 'content'):
                    chunk_text = chunk.content
                else:
                    chunk_text = str(chunk)
                
                # 过滤思考标记
                if not chunk_text:
                    continue
                chunk_text = self._filter_thinking_markers(chunk_text)
                
                if not chunk_text:
                    continue
                
                # 跳过开头可能的英文思考内容
                if not started_output:
                    if any('\u4e00' <= char <= '\u9fff' for char in chunk_text) or chunk_text.startswith('**') or chunk_text.startswith('#'):
                        started_output = True
                        consecutive_english_count = 0
                    else:
                        continue
                else:
                    # 已开始输出后，持续检测是否进入英文思考模式
                    has_chinese = any('\u4e00' <= char <= '\u9fff' for char in chunk_text)
                    
                    if has_chinese:
                        consecutive_english_count = 0
                    else:
                        consecutive_english_count += 1
                        # 连续多个纯英文片段，可能进入思考模式
                        if consecutive_english_count > 5 and self._is_english_thinking(chunk_text):
                            continue
                
                if chunk_text:
                    yield chunk_text
            
        except Exception as e:
            error_msg = f"生成AI结论时发生错误: {str(e)}"
            yield f"❌ {error_msg}"
    
    def _filter_thinking_markers(self, text: str) -> str:
        """过滤LLM输出中的思考标记"""
        import re
        if not text:
            return ""
        text = text.replace('<|endoftext|>', '')
        text = text.replace('<|thinking|>', '')
        text = text.replace('</thinking>', '')
        text = text.replace('<think>', '')
        text = text.replace('</think>', '')
        text = re.sub(r'<\|.*?\|>', '', text)
        return text
    
    def _is_english_thinking(self, text: str) -> bool:
        """检测文本是否为英文思考内容"""
        import re
        if not text or len(text) < 10:
            return False
        
        # 如果包含中文字符，不是纯英文思考
        if any('\u4e00' <= char <= '\u9fff' for char in text):
            return False
        
        # 常见的英文思考关键词
        thinking_keywords = [
            r'\bWrite\b', r'\bsummary\b', r'\btext\b', r'\bfollowing\b',
            r'\bshould\b', r'\binclude\b', r'\banalysis\b', r'\boutline\b',
            r'\bstructure\b', r'\bparagraph\b', r'\bsection\b', r'\bconclusion\b',
            r'\bPlease\b', r'\bplease\b', r'\bNote\b', r'\bnote\b',
            r'\bRemember\b', r'\bMust\b', r'\bAvoid\b', r'\bEnsure\b',
            r'\bI need\b', r'\bI will\b', r'\bLet me\b', r'\bFirst\b',
            r'\bkey\b', r'\bdetailed\b', r'\bessay\b', r'\bword\b',
        ]
        
        match_count = sum(1 for kw in thinking_keywords if re.search(kw, text, re.IGNORECASE))
        if match_count >= 3:
            return True
        
        # 检测是否主要由英文字母组成
        english_chars = sum(1 for c in text if c.isalpha() and ord(c) < 128)
        total_chars = len(text.replace(' ', ''))
        if total_chars > 0 and english_chars / total_chars > 0.7:
            return True
        
        return False
    
    def format_report_to_text(self, report: Dict, stats: Dict = None) -> tuple:
        """
        将研报格式化为文本
        :param report: 报告字典
        :param stats: 统计数据字典（用于AI生成结论）
        :return: (report_text, needs_streaming) 元组，needs_streaming表示是否需要流式生成
        """
        text = f"# {report['title']}\n\n"
        text += f"**生成日期**：{report['generation_date']}\n"
        text += f"**数据时间范围**：{report['data_period']}\n"
        if report.get('conditions_summary'):
            text += f"**筛选条件**：{report['conditions_summary']}\n"
        text += "\n---\n\n"
        
        needs_streaming = False
        # 先添加所有模板生成的内容
        for section in report['sections']:
            if section.get('needs_ai_generation') and section.get('content') is None:
                # 标记需要流式生成，但先添加标题
                needs_streaming = True
                text += f"## {section['title']}\n\n"
            else:
                text += f"## {section['title']}\n\n"
                if section.get('content'):
                    text += f"{section['content']}\n\n"
                text += "---\n\n"
        
        return (text, needs_streaming)
    
    def format_report_to_markdown(self, report: Dict) -> str:
        """将研报格式化为Markdown（带格式）"""
        return self.format_report_to_text(report)

