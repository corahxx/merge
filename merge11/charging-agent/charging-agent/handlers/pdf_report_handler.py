# handlers/pdf_report_handler.py - PDF报告生成处理

from typing import Dict, Optional
from datetime import datetime, date
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT


class PDFReportHandler:
    """处理PDF报告生成逻辑"""
    
    def __init__(self):
        """初始化PDF报告处理器"""
        self.styles = getSampleStyleSheet()
        self._init_custom_styles()
    
    def _init_custom_styles(self):
        """初始化自定义样式"""
        # 标题样式
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1f77b4'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        
        # 信息样式
        self.info_style = ParagraphStyle(
            'InfoStyle',
            parent=self.styles['Normal'],
            fontSize=12,
            spaceAfter=12
        )
    
    def generate_analysis_report(self,
                                stats: Dict,
                                chart_data: Dict,
                                analysis_type: str,
                                date_field: Optional[str] = None,
                                start_date: Optional[date] = None,
                                end_date: Optional[date] = None) -> BytesIO:
        """
        生成数据分析PDF报告
        :param stats: 统计数据
        :param chart_data: 图表数据
        :param analysis_type: 分析类型
        :param date_field: 日期字段
        :param start_date: 起始日期
        :param end_date: 结束日期
        :return: PDF文件字节流
        :raises: Exception 如果生成失败
        """
        try:
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            story = []
            
            # 添加标题
            title = Paragraph("数据分析统计报告", self.title_style)
            story.append(title)
            story.append(Spacer(1, 0.2*inch))
            
            # 添加基本信息
            story.append(Paragraph(f"<b>分析类型：</b>{analysis_type}", self.info_style))
            story.append(Paragraph(f"<b>总记录数：</b>{stats.get('total_records', 0):,}", self.info_style))
            
            if date_field and (start_date or end_date):
                date_range = f"{start_date or '开始'} 至 {end_date or '结束'}"
                story.append(Paragraph(f"<b>时间范围 ({date_field})：</b>{date_range}", self.info_style))
            
            story.append(Spacer(1, 0.3*inch))
            
            # 添加数据表格
            if chart_data and 'dataframe' in chart_data:
                # 优先使用dataframe，更可靠
                df = chart_data['dataframe']
                chart_type = chart_data.get('type', '数据明细')
                story.append(Paragraph(f"<b>{chart_type}明细</b>", self.styles['Heading2']))
                story.append(Spacer(1, 0.1*inch))
                
                # 准备表格数据
                table_data = [['项目', '数量', '占比']]
                total_count = chart_data.get('total', stats.get('total_records', 1))
                
                # 从DataFrame提取数据
                import pandas as pd
                if isinstance(df, pd.DataFrame) and len(df) > 0:
                    for _, row in df.iterrows():
                        # DataFrame第一列是名称，第二列是数量
                        cols = list(df.columns)
                        name = str(row[cols[0]]) if len(cols) > 0 else '未知'
                        count = int(row[cols[1]]) if len(cols) > 1 else 0
                        percentage = (count / total_count * 100) if total_count > 0 else 0
                        table_data.append([name, f"{count:,}", f"{percentage:.2f}%"])
                elif 'data' in chart_data:
                    # 回退到使用data字段
                    for item in chart_data['data']:
                        # 处理字典格式的数据
                        if isinstance(item, dict):
                            keys = list(item.keys())
                            if len(keys) >= 2:
                                name = str(item.get(keys[0], '未知'))
                                count = int(item.get(keys[1], 0))
                            elif len(keys) == 1:
                                name = str(keys[0])
                                count = int(item.get(name, 0))
                            else:
                                name = '未知'
                                count = 0
                        else:
                            name = '未知'
                            count = 0
                        percentage = (count / total_count * 100) if total_count > 0 else 0
                        table_data.append([name, f"{count:,}", f"{percentage:.2f}%"])
                
                if len(table_data) > 1:  # 有数据行（除了表头）
                    # 创建表格
                    table = Table(table_data, colWidths=[3*inch, 1.5*inch, 1.5*inch])
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 12),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ('FONTSIZE', (0, 1), (-1, -1), 10),
                    ]))
                    
                    story.append(table)
                    story.append(Spacer(1, 0.3*inch))
                else:
                    story.append(Paragraph("暂无数据", self.info_style))
            else:
                story.append(Paragraph("暂无图表数据", self.info_style))
            
            # 添加生成时间
            story.append(Spacer(1, 0.5*inch))
            time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            story.append(Paragraph(f"<i>报告生成时间：{time_str}</i>", self.styles['Normal']))
            
            # 构建PDF
            doc.build(story)
            buffer.seek(0)
            return buffer
            
        except Exception as e:
            # 重新抛出异常，让调用者处理
            raise Exception(f"PDF报告生成失败: {str(e)}") from e
    
    def get_report_filename(self, prefix: str = "数据分析报告") -> str:
        """
        生成报告文件名
        :param prefix: 文件名前缀
        :return: 文件名
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{prefix}_{timestamp}.pdf"

