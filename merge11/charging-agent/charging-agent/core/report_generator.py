# core/report_generator.py - 研报生成器

from handlers.industry_report_handler import IndustryReportHandler


class ReportGenerator:
    """研报生成器"""
    
    def __init__(self, table_name: str = 'evdata', llm=None):
        """
        初始化研报生成器
        :param table_name: 数据表名
        :param llm: 可选的大语言模型实例（用于AI生成结论）
        """
        self.table_name = table_name
        self.handler = IndustryReportHandler(table_name, llm=llm)
    
    def generate(self, 
                 question: str = "",
                 limit: int = None,
                 date_field: str = None,
                 start_date = None,
                 end_date = None,
                 operator_filter: list = None) -> tuple:
        """
        生成行业研报
        :param question: 用户问题，用于定制研报内容
        :return: (report_text, report_dict) 元组
        """
        result = self.handler.generate_report(
            question=question,
            limit=limit,
            date_field=date_field,
            start_date=start_date,
            end_date=end_date,
            operator_filter=operator_filter
        )
        
        if result['success']:
            report_dict = result['report']
            stats = result.get('stats', {})
            formatted_result = self.handler.format_report_to_text(report_dict, stats=stats)
            
            # 检查是否需要流式生成
            if isinstance(formatted_result, tuple):
                report_text, needs_streaming = formatted_result
                return (report_text, report_dict, needs_streaming, stats)
            else:
                return (formatted_result, report_dict, False, stats)
        else:
            error_msg = f"生成研报失败: {result.get('error', '未知错误')}"
            return (error_msg, None, False, None)

