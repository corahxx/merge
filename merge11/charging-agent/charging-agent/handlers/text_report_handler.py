# handlers/text_report_handler.py - 文字分析报告生成处理

from typing import Dict, Optional, List
from datetime import datetime, date
import pandas as pd
from data.error_handler import ErrorHandler


class TextReportHandler:
    """处理文字分析报告生成逻辑"""
    
    def __init__(self, llm=None):
        """
        初始化文字报告处理器
        :param llm: LLM模型实例（Ollama），可选
        """
        self.llm = llm
    
    def generate_report(self,
                       stats: Dict,
                       chart_data: Optional[Dict] = None,
                       analysis_type: str = "总体统计",
                       region_filter: Optional[Dict[str, str]] = None,
                       operator_filter: Optional[List[str]] = None,
                       power_filter: Optional[List[str]] = None,
                       date_field: Optional[str] = None,
                       start_date: Optional[date] = None,
                       end_date: Optional[date] = None,
                       use_llm: bool = True) -> Dict:
        """
        生成文字分析报告
        :param stats: 统计数据字典
        :param chart_data: 图表数据字典
        :param analysis_type: 分析类型
        :param region_filter: 区域筛选条件
        :param operator_filter: 运营商筛选条件
        :param power_filter: 功率筛选条件
        :param date_field: 日期字段
        :param start_date: 起始日期
        :param end_date: 结束日期
        :param use_llm: 是否使用LLM生成分析部分
        :return: 报告字典
        """
        try:
            # 1. 提取统计数据摘要（传入region_filter用于智能选择统计级别）
            stats_summary = self._extract_stats_summary(stats, chart_data, region_filter)
            
            # 2. 生成模板部分（核心数据）
            template_part = self._generate_template_part(
                stats, stats_summary, analysis_type,
                region_filter, operator_filter, power_filter,
                date_field, start_date, end_date
            )
            
            # 3. 生成LLM部分（如果启用）
            llm_part = ""
            if use_llm and self.llm:
                try:
                    llm_part = self._generate_llm_part(
                        stats_summary, analysis_type,
                        region_filter, operator_filter, power_filter
                    )
                except Exception as e:
                    # LLM生成失败时，记录错误但不影响整体报告
                    llm_part = f"\n\n> ⚠️ 深度分析生成失败: {str(e)}\n> 报告已包含基础数据分析。\n"
            
            # 4. 组合生成完整报告
            full_report = self._combine_report(template_part, llm_part)
            
            return {
                'success': True,
                'report': full_report,
                'template_part': template_part,
                'llm_part': llm_part,
                'use_llm': use_llm
            }
            
        except Exception as e:
            error_info = ErrorHandler.handle_exception(e, "生成文字分析报告")
            return {
                'success': False,
                'error': error_info.get('error_message', str(e)),
                'error_details': error_info
            }
    
    def _extract_stats_summary(self, stats: Dict, chart_data: Optional[Dict] = None,
                               region_filter: Optional[Dict[str, str]] = None) -> Dict:
        """
        从统计数据中提取关键信息
        :param stats: 统计数据
        :param chart_data: 图表数据
        :param region_filter: 区域筛选条件，用于智能选择统计级别
        :return: 统计摘要字典
        """
        bs = stats.get('basic_stats', {})
        
        # 提取运营商Top 10
        top_operators = []
        if 'by_operator' in bs and bs['by_operator']:
            operator_items = sorted(bs['by_operator'].items(), key=lambda x: x[1], reverse=True)[:10]
            total_operator_count = sum(bs['by_operator'].values())
            for name, count in operator_items:
                percentage = (count / total_operator_count * 100) if total_operator_count > 0 else 0
                top_operators.append({
                    'name': name,
                    'count': count,
                    'percentage': percentage
                })
        
        # 提取区域Top 10（根据region_filter智能选择统计级别）
        top_locations = []
        location_key = None
        location_label = '区域'
        
        # 直辖市列表
        direct_cities = ['北京市', '上海市', '天津市', '重庆市', '北京', '上海', '天津', '重庆']
        
        if region_filter:
            province = region_filter.get('province', '')
            city = region_filter.get('city', '')
            
            # 直辖市特殊处理：选择直辖市时显示区县
            if province in direct_cities or city in direct_cities:
                if 'by_location' in bs and bs['by_location']:
                    location_key = 'by_location'
                    location_label = '区县'
            elif city:
                # 城市级别查询 → 显示区县
                if 'by_location' in bs and bs['by_location']:
                    location_key = 'by_location'
                    location_label = '区县'
            elif province:
                # 省份级别查询 → 显示城市
                if 'by_city' in bs and bs['by_city']:
                    location_key = 'by_city'
                    location_label = '城市'
                elif 'by_location' in bs and bs['by_location']:
                    location_key = 'by_location'
                    location_label = '区县'
        
        # 无筛选或未匹配 → 按优先级选择
        if not location_key:
            if 'by_province' in bs and bs['by_province']:
                location_key = 'by_province'
                location_label = '省份'
            elif 'by_city' in bs and bs['by_city']:
                location_key = 'by_city'
                location_label = '城市'
            elif 'by_location' in bs and bs['by_location']:
                location_key = 'by_location'
                location_label = '区县'
        
        if location_key and bs.get(location_key):
            location_items = sorted(bs[location_key].items(), key=lambda x: x[1], reverse=True)[:10]
            total_location_count = sum(bs[location_key].values())
            for name, count in location_items:
                percentage = (count / total_location_count * 100) if total_location_count > 0 else 0
                top_locations.append({
                    'name': name,
                    'count': count,
                    'percentage': percentage,
                    'label': location_label
                })
        
        # 提取类型Top 5
        top_types = []
        if 'by_type' in bs and bs['by_type']:
            type_items = sorted(bs['by_type'].items(), key=lambda x: x[1], reverse=True)[:5]
            total_type_count = sum(bs['by_type'].values())
            for name, count in type_items:
                percentage = (count / total_type_count * 100) if total_type_count > 0 else 0
                top_types.append({
                    'name': name,
                    'count': count,
                    'percentage': percentage
                })
        
        # 提取功率统计
        power_stats = bs.get('by_power', {})
        
        summary = {
            'total_records': stats.get('total_records', 0),
            'unique_stations': bs.get('unique_stations', 0),
            'unique_piles': bs.get('unique_piles', 0),
            'top_operators': top_operators,
            'top_locations': top_locations,
            'top_types': top_types,
            'power_stats': power_stats,
            'avg_piles_per_station': bs.get('avg_piles_per_station', 0)
        }
        
        return summary
    
    def _generate_template_part(self,
                               stats: Dict,
                               stats_summary: Dict,
                               analysis_type: str,
                               region_filter: Optional[Dict[str, str]],
                               operator_filter: Optional[List[str]],
                               power_filter: Optional[List[str]],
                               date_field: Optional[str],
                               start_date: Optional[date],
                               end_date: Optional[date]) -> str:
        """
        生成模板部分（核心数据）
        """
        lines = []
        
        # 报告标题
        lines.append(f"# {analysis_type}分析报告\n")
        lines.append(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
        lines.append("---\n")
        
        # 一、数据概览
        lines.append("## 一、数据概览\n")
        
        # 基本统计
        lines.append(f"- **总记录数**: {stats_summary['total_records']:,} 条")
        if stats_summary['unique_stations'] > 0:
            lines.append(f"- **唯一充电站数**: {stats_summary['unique_stations']:,} 个")
        if stats_summary['unique_piles'] > 0:
            lines.append(f"- **唯一充电桩数**: {stats_summary['unique_piles']:,} 个")
        if stats_summary['avg_piles_per_station'] > 0:
            lines.append(f"- **平均每站充电桩数**: {stats_summary['avg_piles_per_station']:.1f} 个")
        lines.append("")
        
        # 筛选条件
        filter_info = []
        if date_field and (start_date or end_date):
            date_range = f"{start_date or '开始'} 至 {end_date or '结束'}"
            filter_info.append(f"时间范围 ({date_field}): {date_range}")
        if region_filter:
            region_parts = []
            if region_filter.get('province'):
                region_parts.append(f"省份: {region_filter['province']}")
            if region_filter.get('city'):
                region_parts.append(f"城市: {region_filter['city']}")
            if region_filter.get('district'):
                region_parts.append(f"区县: {region_filter['district']}")
            if region_parts:
                filter_info.append("区域: " + " | ".join(region_parts))
        if operator_filter:
            filter_info.append(f"运营商: {', '.join(operator_filter)}")
        if power_filter:
            filter_info.append(f"功率范围: {', '.join(power_filter)}")
        
        if filter_info:
            lines.append("**筛选条件**:")
            for info in filter_info:
                lines.append(f"- {info}")
            lines.append("")
        
        lines.append("---\n")
        
        # 二、核心统计
        lines.append("## 二、核心统计\n")
        
        # 运营商分布
        if stats_summary['top_operators']:
            lines.append("### 2.1 运营商分布（Top 10）\n")
            for i, op in enumerate(stats_summary['top_operators'], 1):
                lines.append(f"{i}. **{op['name']}**: {op['count']:,} 个 ({op['percentage']:.1f}%)")
            lines.append("")
        
        # 区域分布
        if stats_summary['top_locations']:
            location_label = stats_summary['top_locations'][0]['label'] if stats_summary['top_locations'] else '区域'
            lines.append(f"### 2.2 {location_label}分布（Top 10）\n")
            for i, loc in enumerate(stats_summary['top_locations'], 1):
                lines.append(f"{i}. **{loc['name']}**: {loc['count']:,} 个 ({loc['percentage']:.1f}%)")
            lines.append("")
        
        # 类型分布
        if stats_summary['top_types']:
            lines.append("### 2.3 充电桩类型分布（Top 5）\n")
            for i, typ in enumerate(stats_summary['top_types'], 1):
                lines.append(f"{i}. **{typ['name']}**: {typ['count']:,} 个 ({typ['percentage']:.1f}%)")
            lines.append("")
        
        # 功率统计
        if stats_summary['power_stats']:
            power = stats_summary['power_stats']
            if power.get('total_count', 0) > 0:
                lines.append("### 2.4 额定功率统计\n")
                lines.append(f"- **有效数据量**: {power.get('total_count', 0):,} 个")
                if power.get('mean'):
                    lines.append(f"- **平均功率**: {int(round(power['mean']))} kW")
                if power.get('median'):
                    lines.append(f"- **中位数功率**: {int(round(power['median']))} kW")
                if power.get('min') is not None:
                    lines.append(f"- **最小功率**: {int(round(power['min']))} kW")
                if power.get('max') is not None:
                    lines.append(f"- **最大功率**: {int(round(power['max']))} kW")
                
                # 功率区间分布
                if power.get('by_range'):
                    lines.append("\n**功率区间分布**:")
                    for range_name, count in power['by_range'].items():
                        percentage = (count / power['total_count'] * 100) if power['total_count'] > 0 else 0
                        lines.append(f"- {range_name}: {count:,} 个 ({percentage:.1f}%)")
                lines.append("")
        
        lines.append("---\n")
        
        return "\n".join(lines)
    
    def generate_llm_part_stream(self,
                                 stats_summary: Dict,
                                 analysis_type: str,
                                 region_filter: Optional[Dict[str, str]],
                                 operator_filter: Optional[List[str]],
                                 power_filter: Optional[List[str]],
                                 target_length: int = 2500):
        """
        使用LLM流式生成分析部分
        :param stats_summary: 统计摘要
        :param analysis_type: 分析类型
        :param region_filter: 区域筛选
        :param operator_filter: 运营商筛选
        :param power_filter: 功率筛选
        :param target_length: 目标文字数量（默认2500字）
        :yield: 生成的文本块
        """
        if not self.llm:
            yield ""
            return
        
        # 构建统计数据文本摘要
        stats_text = self._format_stats_for_llm(stats_summary, analysis_type, 
                                               region_filter, operator_filter, power_filter)
        
        # 构建Prompt，根据筛选条件动态调整分析维度
        prompt = self._build_llm_prompt(
            stats_text, analysis_type, target_length=target_length,
            stats_summary=stats_summary, region_filter=region_filter,
            operator_filter=operator_filter, power_filter=power_filter
        )
        
        # 流式调用LLM生成
        try:
            full_text = ""
            buffer = ""  # 累积缓冲区，用于检测连续英文
            started_output = False  # 标记是否已经开始输出有效内容
            consecutive_english_count = 0  # 连续英文片段计数
            
            for chunk in self.llm.stream(prompt):
                chunk_text = chunk.strip() if isinstance(chunk, str) else str(chunk).strip()
                if not chunk_text:
                    continue
                
                # 过滤掉特殊标记和英文思考过程
                chunk_text = self._filter_llm_output(chunk_text)
                
                if not chunk_text:
                    continue
                
                # 检测是否开始输出中文内容（跳过开头的英文思考过程）
                if not started_output:
                    # 检查是否包含中文字符
                    if any('\u4e00' <= char <= '\u9fff' for char in chunk_text):
                        started_output = True
                        consecutive_english_count = 0
                    # 或者检查是否包含Markdown标题标记（##）
                    elif chunk_text.startswith('##') or chunk_text.startswith('###'):
                        started_output = True
                        consecutive_english_count = 0
                    else:
                        # 如果还没有开始输出有效内容，跳过这段
                        continue
                else:
                    # 已开始输出后，持续检测是否进入英文思考模式
                    has_chinese = any('\u4e00' <= char <= '\u9fff' for char in chunk_text)
                    
                    if has_chinese:
                        # 有中文，重置计数器
                        consecutive_english_count = 0
                    else:
                        # 纯英文或特殊字符
                        consecutive_english_count += 1
                        
                        # 如果连续5个以上纯英文片段，可能进入了思考模式，停止输出
                        if consecutive_english_count > 5:
                            # 检查是否是英文思考内容
                            if self._is_english_thinking(chunk_text):
                                # 跳过这段英文思考内容
                                continue
                
                full_text += chunk_text
                yield chunk_text
                
        except Exception as e:
            yield f"\n\n> ⚠️ 深度分析生成失败: {str(e)}\n> 报告已包含基础数据分析。\n"
    
    def _filter_llm_output(self, text: str) -> str:
        """
        过滤LLM输出中的思考过程和特殊标记
        :param text: 原始文本
        :return: 过滤后的文本
        """
        if not text:
            return ""
        
        import re
        
        # 移除特殊标记
        text = text.replace('<|endoftext|>', '')
        text = text.replace('<|thinking|>', '')
        text = text.replace('</thinking>', '')
        text = text.replace('<think>', '')
        text = text.replace('</think>', '')
        
        # 移除 qwen3 模型的思考标记
        text = re.sub(r'<\|.*?\|>', '', text)
        
        # 检查是否主要是英文思考过程
        if self._is_english_thinking(text):
            return ""
        
        return text
    
    def _is_english_thinking(self, text: str) -> bool:
        """
        检测文本是否为英文思考内容
        :param text: 待检测的文本
        :return: True 如果是英文思考内容
        """
        if not text or len(text) < 10:
            return False
        
        import re
        
        # 如果包含中文字符，不是纯英文思考
        if any('\u4e00' <= char <= '\u9fff' for char in text):
            return False
        
        # 常见的英文思考关键词
        thinking_keywords = [
            r'\bWrite\b', r'\bwrite\b', r'\bsummary\b', r'\bSummary\b',
            r'\btext\b', r'\bfollowing\b', r'\bshould\b', r'\binclude\b',
            r'\bmain\b', r'\bpoints\b', r'\banalysis\b', r'\bAnalysis\b',
            r'\boutline\b', r'\bOutline\b', r'\bstructure\b', r'\bStructure\b',
            r'\bparagraph\b', r'\bsection\b', r'\bconclusion\b', r'\bConclusion\b',
            r'\bintroduction\b', r'\bIntroduction\b', r'\bbody\b', r'\bBody\b',
            r'\bthesis\b', r'\bThesis\b', r'\bargument\b', r'\bArgument\b',
            r'\bevidence\b', r'\bEvidence\b', r'\bsource\b', r'\bSource\b',
            r'\breference\b', r'\bReference\b', r'\bcitation\b', r'\bCitation\b',
            r'\bword\b', r'\bWord\b', r'\bwords\b', r'\bWords\b',
            r'\bcharacter\b', r'\bCharacter\b', r'\blength\b', r'\bLength\b',
            r'\brequirement\b', r'\bRequirement\b', r'\bformat\b', r'\bFormat\b',
            r'\bPlease\b', r'\bplease\b', r'\bNote\b', r'\bnote\b',
            r'\bRemember\b', r'\bremember\b', r'\bMust\b', r'\bmust\b',
            r'\bDon\'t\b', r'\bdon\'t\b', r'\bAvoid\b', r'\bavoid\b',
            r'\bEnsure\b', r'\bensure\b', r'\bMake sure\b', r'\bmake sure\b',
            r'\bI need\b', r'\bi need\b', r'\bI will\b', r'\bi will\b',
            r'\bI\'ll\b', r'\bi\'ll\b', r'\bLet me\b', r'\blet me\b',
            r'\bFirst\b', r'\bfirst\b', r'\bSecond\b', r'\bsecond\b',
            r'\bThird\b', r'\bthird\b', r'\bFinally\b', r'\bfinally\b',
            r'\bHere\b', r'\bhere\b', r'\bThis\b', r'\bthis\b',
            r'\bThe\b', r'\bthe\b', r'\ba\b', r'\bA\b',
            r'\bkey\b', r'\bKey\b', r'\bfind\b', r'\bFind\b',
            r'\bdetailed\b', r'\bDetailed\b', r'\bessay\b', r'\bEssay\b',
            # 常见的英文连词和介词
            r'\band\b', r'\bor\b', r'\bbut\b', r'\bwith\b', r'\bfor\b',
            r'\bfrom\b', r'\binto\b', r'\babout\b', r'\bthrough\b',
        ]
        
        # 计算匹配的关键词数量
        match_count = 0
        for keyword in thinking_keywords:
            if re.search(keyword, text):
                match_count += 1
        
        # 如果匹配超过3个英文关键词，认为是思考内容
        if match_count >= 3:
            return True
        
        # 检测是否主要由英文单词组成（超过50%是英文字母）
        english_chars = sum(1 for c in text if c.isalpha() and ord(c) < 128)
        total_chars = len(text.replace(' ', ''))
        if total_chars > 0 and english_chars / total_chars > 0.7:
            # 70%以上是英文字母，且没有中文，很可能是思考内容
            return True
        
        return False
    
    def _generate_llm_part(self,
                          stats_summary: Dict,
                          analysis_type: str,
                          region_filter: Optional[Dict[str, str]],
                          operator_filter: Optional[List[str]],
                          power_filter: Optional[List[str]]) -> str:
        """
        使用LLM生成分析部分（非流式，用于兼容）
        """
        if not self.llm:
            return ""
        
        # 构建统计数据文本摘要
        stats_text = self._format_stats_for_llm(stats_summary, analysis_type, 
                                               region_filter, operator_filter, power_filter)
        
        # 构建Prompt，根据筛选条件动态调整分析维度
        prompt = self._build_llm_prompt(
            stats_text, analysis_type, target_length=2500,
            stats_summary=stats_summary, region_filter=region_filter,
            operator_filter=operator_filter, power_filter=power_filter
        )
        
        # 调用LLM生成
        try:
            response = self.llm.invoke(prompt)
            llm_content = response.strip() if isinstance(response, str) else str(response)
            
            # 格式化LLM生成的内容
            formatted_content = self._format_llm_content(llm_content)
            
            return formatted_content
        except Exception as e:
            raise Exception(f"LLM生成失败: {str(e)}")
    
    def _format_stats_for_llm(self,
                             stats_summary: Dict,
                             analysis_type: str,
                             region_filter: Optional[Dict[str, str]],
                             operator_filter: Optional[List[str]],
                             power_filter: Optional[List[str]]) -> str:
        """
        将统计数据格式化为LLM可理解的文本
        """
        lines = []
        
        lines.append(f"分析类型: {analysis_type}")
        lines.append(f"总记录数: {stats_summary['total_records']:,} 条")
        lines.append(f"唯一充电站数: {stats_summary['unique_stations']:,} 个")
        lines.append(f"唯一充电桩数: {stats_summary['unique_piles']:,} 个")
        lines.append("")
        
        # 运营商信息
        if stats_summary['top_operators']:
            lines.append("运营商分布（Top 5）:")
            for op in stats_summary['top_operators']:
                lines.append(f"  - {op['name']}: {op['count']:,} 个 ({op['percentage']:.1f}%)")
            lines.append("")
        
        # 区域信息
        if stats_summary['top_locations']:
            location_label = stats_summary['top_locations'][0]['label'] if stats_summary['top_locations'] else '区域'
            lines.append(f"{location_label}分布（Top 5）:")
            for loc in stats_summary['top_locations']:
                lines.append(f"  - {loc['name']}: {loc['count']:,} 个 ({loc['percentage']:.1f}%)")
            lines.append("")
        
        # 类型信息
        if stats_summary['top_types']:
            lines.append("充电桩类型分布（Top 5）:")
            for typ in stats_summary['top_types']:
                lines.append(f"  - {typ['name']}: {typ['count']:,} 个 ({typ['percentage']:.1f}%)")
            lines.append("")
        
        # 功率信息
        if stats_summary['power_stats']:
            power = stats_summary['power_stats']
            if power.get('total_count', 0) > 0:
                lines.append("额定功率统计:")
                lines.append(f"  平均功率: {int(round(power.get('mean', 0)))} kW")
                lines.append(f"  中位数功率: {int(round(power.get('median', 0)))} kW")
                if power.get('by_range'):
                    lines.append("  功率区间分布:")
                    for range_name, count in power['by_range'].items():
                        percentage = (count / power['total_count'] * 100) if power['total_count'] > 0 else 0
                        lines.append(f"    - {range_name}: {count:,} 个 ({percentage:.1f}%)")
                lines.append("")
        
        # 筛选条件
        filter_parts = []
        if region_filter:
            if region_filter.get('province'):
                filter_parts.append(f"省份: {region_filter['province']}")
            if region_filter.get('city'):
                filter_parts.append(f"城市: {region_filter['city']}")
            if region_filter.get('district'):
                filter_parts.append(f"区县: {region_filter['district']}")
        if operator_filter:
            filter_parts.append(f"运营商: {', '.join(operator_filter)}")
        if power_filter:
            filter_parts.append(f"功率范围: {', '.join(power_filter)}")
        
        if filter_parts:
            lines.append("筛选条件:")
            for part in filter_parts:
                lines.append(f"  - {part}")
        
        return "\n".join(lines)
    
    def _build_llm_prompt(self, stats_text: str, analysis_type: str, target_length: int = 2500,
                          stats_summary: Dict = None, region_filter: Dict = None,
                          operator_filter: List = None, power_filter: List = None) -> str:
        """
        构建LLM Prompt - 根据查询条件动态调整分析维度
        :param stats_text: 格式化的统计数据文本
        :param analysis_type: 分析类型
        :param target_length: 目标字数（默认2500字）
        :param stats_summary: 统计摘要字典
        :param region_filter: 区域筛选条件
        :param operator_filter: 运营商筛选条件
        :param power_filter: 功率筛选条件
        """
        # 根据筛选条件确定分析维度和重点
        analysis_dimensions = self._determine_analysis_dimensions(
            stats_summary, region_filter, operator_filter, power_filter
        )
        
        # 构建动态分析框架
        analysis_framework = self._build_analysis_framework(
            analysis_dimensions, stats_summary, region_filter, operator_filter, power_filter
        )
        
        prompt = f"""你是一位资深的电动汽车充电基础设施行业分析师，具备深厚的行业洞察力和数据分析能力。

## 【角色定位】
- 熟悉中国充电桩行业的发展历程、政策环境和市场格局
- 了解主要运营商的竞争策略和市场表现
- 对不同区域市场的发展特点有深入理解
- 熟悉充电设备的技术参数和应用场景

## 【数据背景】
{stats_text}

## 【分析任务】
分析类型：{analysis_type}

{analysis_framework}

## 【写作要求】

### 内容深度要求
1. **数据驱动**：所有观点必须基于提供的数据，用具体数字支撑论点
2. **因果分析**：不仅描述现象，更要分析背后的原因和驱动因素
3. **对比分析**：善用对比（头部vs尾部、不同类型、不同区域）揭示差异
4. **趋势判断**：基于数据特征推断行业发展趋势

### 语言风格要求
1. 专业术语准确，如"市场集中度CR3/CR5"、"存量/增量市场"、"充电服务网络"
2. 行文流畅，逻辑严密，避免简单堆砌数据
3. 善用过渡词和总结句，使段落之间衔接自然
4. 适当使用行业视角，如政策影响、技术趋势、用户需求变化

### 格式要求
- 使用Markdown格式，标题层级清晰
- 使用**加粗**突出关键数据和核心观点
- 适当使用列表提高可读性
- 段落长度适中，每段聚焦一个主题
- **总字数约{target_length}字**

### 禁止事项
- 禁止输出思考过程或任何英文内容
- 禁止重复已在"核心统计"部分列出的原始数据
- 禁止使用空泛的套话，必须结合具体数据
- 禁止虚构数据或提供未经数据支撑的结论

## 【输出结构】

## 三、深度数据洞察

### 3.1 核心发现
（基于数据提炼4-6条关键发现，每条包含数据支撑和简要分析）

### 3.2 {analysis_dimensions['primary_dimension']}
（深入分析主要维度的特征、格局和趋势，约600-800字）

### 3.3 {analysis_dimensions['secondary_dimension']}
（分析次要维度的分布特点和关键洞察，约400-600字）

{analysis_dimensions.get('extra_section', '')}

## 四、战略建议与展望

### 4.1 行业格局研判
（基于数据分析当前市场格局特点，约300字）

### 4.2 发展建议
（针对运营商/投资者/政策制定者提出3-5条可操作建议，约400字）

### 4.3 风险提示
（识别数据中反映的潜在风险点，约200字）

---

请直接开始输出分析报告，不要输出任何思考过程："""
        return prompt
    
    def _determine_analysis_dimensions(self, stats_summary: Dict = None, 
                                       region_filter: Dict = None,
                                       operator_filter: List = None,
                                       power_filter: List = None) -> Dict:
        """
        根据查询条件确定分析维度
        """
        dimensions = {
            'primary_dimension': '市场竞争格局分析',
            'secondary_dimension': '区域分布特征分析',
            'extra_section': '',
            'focus_points': []
        }
        
        # 根据筛选条件调整分析重点
        if operator_filter and len(operator_filter) > 0:
            # 指定运营商筛选 → 重点分析该运营商的区域布局和设备特征
            dimensions['primary_dimension'] = '运营商区域布局深度分析'
            dimensions['secondary_dimension'] = '设备结构与技术特征分析'
            dimensions['focus_points'] = ['区域渗透率', '设备类型偏好', '功率配置策略', '市场覆盖广度']
            dimensions['extra_section'] = '''
### 3.4 竞争力评估
（分析该运营商的市场竞争力和差异化特征，约300字）'''
        
        elif region_filter:
            province = region_filter.get('province', '')
            city = region_filter.get('city', '')
            district = region_filter.get('district', '')
            
            if district:
                # 区县级筛选 → 重点分析微观市场特征
                dimensions['primary_dimension'] = '区域市场微观分析'
                dimensions['secondary_dimension'] = '运营商竞争态势分析'
                dimensions['focus_points'] = ['市场饱和度', '运营商渗透情况', '设备服务能力', '场景覆盖']
                dimensions['extra_section'] = '''
### 3.4 服务能力评估
（分析该区域的充电服务能力和用户需求匹配度，约300字）'''
            elif city:
                # 城市级筛选 → 重点分析城市充电网络布局
                dimensions['primary_dimension'] = '城市充电网络布局分析'
                dimensions['secondary_dimension'] = '运营商市场份额分析'
                dimensions['focus_points'] = ['区县覆盖均衡性', '头部运营商格局', '快慢充配比', '重点区域识别']
                dimensions['extra_section'] = '''
### 3.4 区县发展差异分析
（分析城市内各区县的发展差异和潜力区域，约300字）'''
            elif province:
                # 省级筛选 → 重点分析省内城市分布差异
                dimensions['primary_dimension'] = '省内城市分布格局分析'
                dimensions['secondary_dimension'] = '运营商省内竞争态势'
                dimensions['focus_points'] = ['城市发展梯队', '区域集中度', '头部城市特征', '发展潜力城市']
                dimensions['extra_section'] = '''
### 3.4 城市梯队分析
（将省内城市按充电桩规模分层，分析各梯队特征，约300字）'''
        
        elif power_filter and len(power_filter) > 0:
            # 功率筛选 → 重点分析特定功率设备的分布
            dimensions['primary_dimension'] = '充电设备功率结构分析'
            dimensions['secondary_dimension'] = '区域功率配置差异分析'
            dimensions['focus_points'] = ['功率分布特征', '运营商技术路线', '应用场景匹配', '升级趋势']
            dimensions['extra_section'] = '''
### 3.4 应用场景分析
（分析该功率区间设备的典型应用场景和用户需求，约300字）'''
        
        else:
            # 全量数据 → 全面分析
            dimensions['primary_dimension'] = '市场竞争格局深度分析'
            dimensions['secondary_dimension'] = '区域发展不均衡性分析'
            dimensions['focus_points'] = ['头部运营商格局', '省份发展差异', '技术路线分布', '功率结构特征']
            dimensions['extra_section'] = '''
### 3.4 技术发展趋势
（分析充电桩类型和功率分布反映的技术发展趋势，约300字）'''
        
        return dimensions
    
    def _build_analysis_framework(self, dimensions: Dict, stats_summary: Dict = None,
                                  region_filter: Dict = None, operator_filter: List = None,
                                  power_filter: List = None) -> str:
        """
        构建动态分析框架说明
        """
        focus_points = dimensions.get('focus_points', [])
        focus_text = '、'.join(focus_points) if focus_points else '市场格局、区域分布、设备结构'
        
        # 构建分析背景说明
        context_parts = []
        
        if operator_filter:
            context_parts.append(f"本次分析聚焦于【{', '.join(operator_filter)}】运营商")
        
        if region_filter:
            region_parts = []
            if region_filter.get('province'):
                region_parts.append(region_filter['province'])
            if region_filter.get('city'):
                region_parts.append(region_filter['city'])
            if region_filter.get('district'):
                region_parts.append(region_filter['district'])
            if region_parts:
                context_parts.append(f"地理范围限定为【{' > '.join(region_parts)}】")
        
        if power_filter:
            context_parts.append(f"功率范围筛选为【{', '.join(power_filter)}】")
        
        context_text = '，'.join(context_parts) if context_parts else "本次为全量数据综合分析"
        
        # 提取关键指标用于分析指引
        guidance = f"""
### 分析背景
{context_text}。

### 分析重点
请围绕以下维度展开深入分析：**{focus_text}**

### 分析指引
1. **市场规模视角**：分析充电站和充电桩的数量规模，评估市场发展阶段
2. **竞争格局视角**：分析运营商的市场份额分布，判断市场集中度和竞争态势
3. **区域分布视角**：分析地理分布的均衡性，识别发展高地和潜力区域
4. **技术结构视角**：分析充电桩类型和功率分布，判断技术路线和用户需求匹配度
"""
        return guidance
    
    def _format_llm_content(self, content: str) -> str:
        """
        格式化LLM生成的内容
        """
        # 确保内容以换行开始
        if not content.startswith('\n'):
            content = '\n' + content
        
        return content
    
    def _combine_report(self, template_part: str, llm_part: str) -> str:
        """
        组合生成完整报告
        """
        if llm_part:
            return template_part + llm_part
        else:
            return template_part
    
    def generate_template_analysis(self,
                                   stats_summary: Dict,
                                   analysis_type: str,
                                   region_filter: Optional[Dict[str, str]] = None,
                                   operator_filter: Optional[List[str]] = None,
                                   power_filter: Optional[List[str]] = None) -> str:
        """
        基于统计数据生成模板化深度分析（不使用LLM）
        完全基于数据驱动，无需AI推理
        
        :param stats_summary: 统计摘要字典
        :param analysis_type: 分析类型
        :param region_filter: 区域筛选条件
        :param operator_filter: 运营商筛选条件
        :param power_filter: 功率筛选条件
        :return: Markdown格式的分析文本
        """
        lines = []
        
        # 3.1 核心发现
        lines.append("### 3.1 核心发现\n")
        lines.append("基于以上统计数据，本次分析得出以下核心发现：\n")
        
        # 市场规模
        total = stats_summary.get('total_records', 0)
        stations = stats_summary.get('unique_stations', 0)
        piles = stats_summary.get('unique_piles', 0)
        avg_per_station = piles / stations if stations > 0 else 0
        
        lines.append(f"1. **市场规模**：共统计充电桩 **{piles:,}** 台，覆盖 **{stations:,}** 座充电站，"
                    f"平均每站配置 **{avg_per_station:.1f}** 台充电桩。\n")
        
        # 运营商格局
        top_ops = stats_summary.get('top_operators', [])
        if top_ops:
            top1 = top_ops[0]
            lines.append(f"2. **运营商格局**：\n")
            lines.append(f"   - 头部运营商：**{top1['name']}** 以 {top1['count']:,} 台（占比 {top1['percentage']:.1f}%）位居第一\n")
            
            if len(top_ops) >= 3:
                cr3 = sum(op['percentage'] for op in top_ops[:3])
                lines.append(f"   - CR3（前三市场份额）：**{cr3:.1f}%**\n")
            if len(top_ops) >= 5:
                cr5 = sum(op['percentage'] for op in top_ops[:5])
                lines.append(f"   - CR5（前五市场份额）：**{cr5:.1f}%**\n")
            
            # 市场集中度判断
            if len(top_ops) >= 3:
                cr3 = sum(op['percentage'] for op in top_ops[:3])
                if cr3 > 50:
                    lines.append(f"   - 市场呈现**高度集中**特征，头部效应明显\n")
                elif cr3 > 30:
                    lines.append(f"   - 市场呈现**中度集中**特征\n")
                else:
                    lines.append(f"   - 市场呈现**分散竞争**格局\n")
            lines.append("\n")
        
        # 区域分布
        top_locs = stats_summary.get('top_locations', [])
        if top_locs:
            loc_label = top_locs[0].get('label', '区域') if top_locs else '区域'
            top1_loc = top_locs[0]
            lines.append(f"3. **{loc_label}分布**：\n")
            lines.append(f"   - 排名第一：**{top1_loc['name']}**（{top1_loc['count']:,} 台，占比 {top1_loc['percentage']:.1f}%）\n")
            
            if len(top_locs) >= 5:
                top5_pct = sum(loc['percentage'] for loc in top_locs[:5])
                lines.append(f"   - 前五{loc_label}合计占比：**{top5_pct:.1f}%**\n")
                if top5_pct > 50:
                    lines.append(f"   - 区域发展**不均衡特征明显**\n")
                else:
                    lines.append(f"   - 区域分布**相对均衡**\n")
            lines.append("\n")
        
        # 设备结构
        top_types = stats_summary.get('top_types', [])
        power_stats = stats_summary.get('power_stats', {})
        
        if top_types:
            top1_type = top_types[0]
            lines.append(f"4. **设备结构**：\n")
            lines.append(f"   - 主流类型：**{top1_type['name']}**（占比 {top1_type['percentage']:.1f}%）\n")
            
            if power_stats and power_stats.get('by_range'):
                by_range = power_stats['by_range']
                max_range = max(by_range.items(), key=lambda x: x[1])
                total_power = power_stats.get('total_count', 1)
                max_pct = max_range[1] / total_power * 100 if total_power > 0 else 0
                lines.append(f"   - 主流功率区间：**{max_range[0]}**（占比 {max_pct:.1f}%）\n")
            
            # 快慢充判断
            dc_count = sum(t['count'] for t in top_types if '直流' in t['name'] or '快充' in t['name'])
            ac_count = sum(t['count'] for t in top_types if '交流' in t['name'] or '慢充' in t['name'])
            if dc_count > ac_count:
                lines.append(f"   - **快充桩占比高于慢充桩**，符合快速补电需求\n")
            elif ac_count > dc_count:
                lines.append(f"   - **慢充桩占比较高**，适合居民区/办公区场景\n")
            lines.append("\n")
        
        # 3.2 分析建议
        lines.append("### 3.2 分析建议\n")
        
        suggestion_num = 1
        
        # 根据筛选条件给出针对性建议
        if operator_filter:
            lines.append(f"{suggestion_num}. **运营商视角**：关注当前运营商在各区域的布局密度，"
                        f"识别覆盖空白区域和潜力市场。\n")
            suggestion_num += 1
        
        if region_filter:
            region_name = region_filter.get('district') or region_filter.get('city') or region_filter.get('province', '该区域')
            lines.append(f"{suggestion_num}. **区域视角**：{region_name}市场中可关注头部运营商的竞争策略，"
                        f"以及快慢充配比是否满足本地需求。\n")
            suggestion_num += 1
        
        if not operator_filter and not region_filter:
            lines.append(f"{suggestion_num}. **运营商视角**：头部效应明显，新进入者需差异化竞争策略。\n")
            suggestion_num += 1
            lines.append(f"{suggestion_num}. **区域视角**：中西部地区充电桩密度较低，具备发展空间。\n")
            suggestion_num += 1
        
        lines.append(f"{suggestion_num}. **技术视角**：关注充电设备功率升级趋势，"
                    f"评估现有设备是否满足新能源汽车续航提升需求。\n")
        
        lines.append("\n---\n")
        
        return "\n".join(lines)

