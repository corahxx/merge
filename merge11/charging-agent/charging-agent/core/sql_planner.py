# core/sql_planner.py
class SQLPlanner:
    def __init__(self, llm, db, context):
        from core.intent_classifier import IntentClassifier
        from core.sql_generator import SQLGenerator
        from core.query_executor import QueryExecutor
        from core.response_formatter import ResponseFormatter

        self.intent = IntentClassifier()
        self.generator = SQLGenerator(llm, db)
        self.executor = QueryExecutor(db)
        self.formatter = ResponseFormatter()
        self.context = context

    def classify_intent(self, q: str) -> str:
        return self.intent.classify(q)

    def execute(self, question: str) -> dict:
        hint = "\n".join([f"历史：{q}" for q in self.context.get_last_few_questions()])
        sql = self.generator.generate(question, hint)
        if not sql or not sql.strip().upper().startswith("SELECT"):
            raise RuntimeError("无法生成有效SQL")
        result = self.executor.run(sql)
        # 将SQL语句和结果都保存到字典中
        return {'_result': result, '_sql': sql}
    
    def generate_sql(self, question: str) -> str:
        """生成SQL语句（不执行）"""
        hint = "\n".join([f"历史：{q}" for q in self.context.get_last_few_questions()])
        return self.generator.generate(question, hint)

    def format_result(self, question: str, result: dict) -> str:
        # 从result字典中提取实际的查询结果（_result字段）
        actual_result = result.get('_result', result)
        return self.formatter.format(question, actual_result)
