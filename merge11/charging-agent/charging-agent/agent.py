# agent.py - 充电桩AI助手主类 | 极简流程层 | 模块化架构

from datetime import datetime


class ChargingDataAgent:
    """
    主智能体：仅负责流程编排，所有逻辑下沉至 core/
    """

    def __init__(self, llm, db, verbose=True, table_name: str = 'evdata'):
        from core.orchestrator import Orchestrator
        self.orchestrator = Orchestrator(llm, db, verbose=verbose, table_name=table_name)
        self.verbose = verbose

    def ask(self, question: str, debug=False) -> tuple:
        """
        接收问题，返回回答、SQL语句和思考过程
        :param question: 用户问题
        :param debug: 是否调试模式
        :return: (answer, sql, thinking_steps, report_dict) 元组，sql和report_dict可能为None
        """
        start_time = datetime.now()
        if self.verbose:
            print(f"\n🎯 [{start_time.strftime('%H:%M:%S')}] 提问：{question}")

        try:
            result = self.orchestrator.run(question, debug=debug)
            # 处理返回结果（可能是4元组或6元组）
            if len(result) == 6:
                response, sql, thinking_steps, report_dict, needs_streaming, stats = result
            elif len(result) == 4:
                response, sql, thinking_steps, report_dict = result
                needs_streaming = False
                stats = None
            else:
                response, sql, thinking_steps = result[0], result[1] if len(result) > 1 else None, result[2] if len(result) > 2 else []
                report_dict = None
                needs_streaming = False
                stats = None
        except Exception as e:
            response = f"❌ 系统异常：{str(e)}"
            sql = None
            thinking_steps = [("错误", f"处理过程中发生错误: {str(e)}")]
            report_dict = None
            needs_streaming = False
            stats = None
            if self.verbose:
                print(response)

        duration = (datetime.now() - start_time).total_seconds()
        if self.verbose and "⏱️" not in response:
            response += f" \n⏱️ （耗时 {duration:.2f}s）"
        
        # 添加耗时信息到思考过程
        if thinking_steps:
            thinking_steps.append(("完成", f"总耗时: {duration:.2f}秒"))

        return (response, sql, thinking_steps, report_dict, needs_streaming, stats)

    def clear_history(self):
        self.orchestrator.context.clear()

    @property
    def history(self):
        return self.orchestrator.context.history.copy()
