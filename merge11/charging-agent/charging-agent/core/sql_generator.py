# core/sql_generator.py - 充电桩AI助手 | SQL生成器 | 增强版
import re
from langchain_core.prompts import PromptTemplate
from .schema_inspector import SchemaInspector
from core.knowledge_base import KnowledgeBase


class SQLGenerator:
    def __init__(self, llm, db):
        """
        初始化SQL生成器
        :param llm: LangChain LLM 实例（如 Ollama）
        :param db: SQLDatabase 实例
        """
        self.llm = llm
        self.db = db
        self.schema_prompt = SchemaInspector.get_schema_prompt(db)

    def generate(self, question: str, context_hint: str = "") -> str:
        """
        根据用户问题和上下文生成SQL查询语句
        :param question: 用户输入的问题
        :param context_hint: 上下文提示（如历史问答摘要）
        :return: 生成的SQL字符串
        """
        # === 步骤1：从问题中提取关键词映射 ===
        keywords = []
        for field, terms in KnowledgeBase.FIELD_MAPPING.items():
            for term in terms:
                if term in question:
                    actual_val = KnowledgeBase.get_actual_value(field, term)
                    if actual_val != term:
                        keywords.append(f"【{term}】→ 字段：{field}，应匹配为：'{actual_val}'")
                    else:
                        keywords.append(f"【{term}】→ 字段：{field}")

        context_notes = "\n".join(keywords) if keywords else "未检测到特殊简称。"

        # === 步骤2：构建完整的上下文信息 ===
        full_context_parts = []

        # 加入对话上下文（如有）
        if context_hint and "第一轮" not in context_hint:
            full_context_parts.append("【对话上下文】\n" + context_hint.strip())

        # 加入当前意图分析
        full_context_parts.append("【当前意图分析】\n" + context_notes)

        full_context = "\n\n".join(filter(None, full_context_parts))

        # === 步骤3：构造 Prompt 并调用模型 ===
        prompt_template = PromptTemplate.from_template("""
你是一位资深的MySQL数据分析专家，正在协助完成充电业务的数据查询任务。

{schema}

【用户当前问题】
{question}

{full_context}

【任务要求】
- 分析关键词并正确映射到数据库字段
- 使用 LIKE 实现模糊匹配（如 '%星星充电%'）
- 时间条件优先使用 DATE(充电开始时间)
- 只返回 SELECT 查询语句本身，不要解释，不要加 ```sql
- 确保语法正确，避免拼写错误
        """.strip())

        prompt = prompt_template.format(
            schema=self.schema_prompt,
            question=question,
            full_context=full_context
        )

        raw_output = self.llm.invoke(prompt).strip()

        # === 步骤4：安全提取 SQL ===
        return self._extract_sql(raw_output)

    def _extract_sql(self, text: str) -> str:
        """
        多策略安全提取 SQL 语句
        :param text: AI 的原始输出文本
        :return: 提取出的 SQL 语句
        """
        # 策略1：标准 SELECT ... ;
        match = re.search(r'(SELECT\s+[\s\S]+?;)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # 策略2：无分号补全
        match = re.search(r'(SELECT\s+[\s\S]+?)(?:\n\n|--|$)', text, re.IGNORECASE)
        if match:
            partial = match.group(1).strip()
            if partial.upper().startswith("SELECT"):
                return partial + ";"

        # 策略3：代码块包裹
        match = re.search(r'```(?:sql)?\s*(SELECT[\s\S]+?)\s*;?\s*```', text, re.IGNORECASE)
        if match:
            return match.group(1).strip() + ";"

        # 策略4：查找第一个 SELECT 并截断
        idx = text.upper().find("SELECT")
        if idx != -1:
            try:
                sql_part = text[idx:]
                end_semicolon = sql_part.find(";")
                if end_semicolon != -1:
                    return sql_part[:end_semicolon + 1].strip()
                else:
                    return sql_part.split("\n")[0].strip() + ";"  # 行末截断
            except:
                pass

        # 所有策略失败 → 抛出异常
        raise ValueError(f"未能从AI响应中提取出有效SQL：{text}")
