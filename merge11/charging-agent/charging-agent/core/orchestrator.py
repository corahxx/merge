# core/orchestrator.py
class Orchestrator:
    def __init__(self, llm, db, verbose=True, table_name: str = 'evdata'):
        from core.context_manager import ContextManager
        from core.redirector import Redirector
        from core.sql_planner import SQLPlanner
        from core.ai_responder import AIResponder
        from core.report_generator import ReportGenerator

        self.context = ContextManager(verbose=verbose)
        self.redirector = Redirector(self.context)
        self.planner = SQLPlanner(llm, db, self.context)
        self.ai_reply = AIResponder(llm, verbose=verbose)
        self.report_generator = ReportGenerator(table_name=table_name, llm=llm)

    def run(self, question: str, debug=False) -> tuple:
        """
        运行查询，返回回答、SQL语句和思考过程
        :param question: 用户问题
        :param debug: 是否调试模式
        :return: (answer, sql, thinking_steps, report_dict) 元组，sql和report_dict可能为None
        """
        sql = None
        thinking_steps = []
        
        # 路径1：是否是"那XX呢？"这类跟进句？
        if self.redirector.should_redirect(question):
            thinking_steps.append(("意图理解", "检测到跟进式查询，正在重定向到前一个问题"))
            new_q = self.redirector.build_query(question)
            if new_q:
                thinking_steps.append(("问题重定向", f"原问题: {question}\n重定向为: {new_q}"))
                thinking_steps.append(("生成SQL", "正在生成SQL查询语句..."))
                result = self.planner.execute(new_q)
                sql = result.get('_sql')
                if sql:
                    thinking_steps.append(("SQL生成完成", f"```sql\n{sql}\n```"))
                thinking_steps.append(("执行查询", "正在执行SQL查询..."))
                answer = self.planner.format_result(new_q, result)
                thinking_steps.append(("格式化结果", "正在格式化查询结果..."))
                self.context.add_interaction(new_q, answer)
                if debug:
                    return (f"{answer}\n\n🔧 [重定向] 原问题：{question}", sql, thinking_steps, None, False, None)
                return (answer, sql, thinking_steps, None, False, None)

        # 路径2：查数据库？还是生成研报？
        thinking_steps.append(("意图分类", "正在分析用户意图..."))
        intent = self.planner.classify_intent(question)
        
        if intent == "report":
            thinking_steps.append(("意图识别", "识别为行业研报生成请求"))
            
            # 先解析条件，以便在思考过程中显示
            from core.condition_parser import ConditionParser
            condition_parser = ConditionParser(table_name=self.report_generator.table_name)
            parsed_conditions = condition_parser.parse_conditions(question)
            
            if parsed_conditions.get('parsed_conditions'):
                conditions_summary = "；".join(parsed_conditions['parsed_conditions'])
                thinking_steps.append(("分析条件", f"从问题中解析到以下筛选条件：{conditions_summary}"))
                
                # 如果有区域筛选，显示区域级别判断结果
                if parsed_conditions.get('region_filter'):
                    region_filter = parsed_conditions['region_filter']
                    region_info = []
                    if region_filter.get('province'):
                        region_info.append(f"省份级别: {region_filter['province']}")
                    if region_filter.get('city'):
                        region_info.append(f"城市级别: {region_filter['city']}")
                    if region_filter.get('district'):
                        region_info.append(f"区县级别: {region_filter['district']}")
                    if region_info:
                        thinking_steps.append(("区域级别判断", "已识别区域级别，将使用对应字段进行精准检索：\n" + "\n".join(region_info)))
            else:
                thinking_steps.append(("分析条件", "未检测到特定筛选条件，将使用全量数据"))
            
            thinking_steps.append(("查询数据", "根据解析的条件查询数据库..."))
            thinking_steps.append(("生成研报", "基于查询结果生成定制化行业研报..."))
            result = self.report_generator.generate(question=question)
            if len(result) == 4:
                report_text, report_dict, needs_streaming, stats = result
            else:
                # 兼容旧格式
                report_text, report_dict = result[0], result[1] if len(result) > 1 else None
                needs_streaming = False
                stats = result[3] if len(result) > 3 else None
            
            thinking_steps.append(("研报生成完成", "已基于筛选条件和用户问题生成定制化行业研报"))
            self.context.add_interaction(question, "已生成行业研报")
            # 返回报告文本、SQL、思考过程、报告字典、是否需要流式生成和统计数据
            return (report_text, None, thinking_steps, report_dict, needs_streaming, stats)
        
        if intent == "query":
            thinking_steps.append(("意图识别", f"识别为数据库查询请求"))
            thinking_steps.append(("生成SQL", "正在分析问题并生成SQL查询语句..."))
            result = self.planner.execute(question)
            sql = result.get('_sql')
            if sql:
                thinking_steps.append(("SQL生成完成", f"```sql\n{sql}\n```"))
            thinking_steps.append(("执行查询", "正在连接数据库并执行查询..."))
            answer = self.planner.format_result(question, result)
            thinking_steps.append(("格式化结果", "正在将查询结果转换为自然语言回答..."))
            self.context.add_interaction(question, answer)
            if debug:
                return (f"{answer}\n\n📊 [调试] 数据={result}", sql, thinking_steps, None, False, None)
            return (answer, sql, thinking_steps, None, False, None)

        # 路径3：走AI回复
        thinking_steps.append(("意图识别", "识别为普通对话，使用AI回复"))
        thinking_steps.append(("AI思考", "正在生成回复..."))
        answer = self.ai_reply.reply(question)
        thinking_steps.append(("回复生成完成", "AI已生成回复"))
        self.context.add_interaction(question, answer)
        return (answer, None, thinking_steps, None, False, None)
