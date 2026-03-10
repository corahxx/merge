# core/conversation_context.py
from typing import List, Tuple, Optional
import json


class ConversationContext:
    """
    对话上下文管理器
    负责维护用户对话状态、提取意图、生成增强提示
    """

    def __init__(self, user_id: str = "default"):
        self.user_id = user_id
        self.messages: List[Tuple[str, str]] = []  # [(role, content), ...]
        self.last_sql: Optional[str] = None
        self.last_result: Optional[str] = None
        self.current_intent = None

    def add_message(self, role: str, content: str):
        """添加一条消息"""
        self.messages.append((role, content))

    def get_history(self, limit: int = 5) -> List[Tuple[str, str]]:
        """获取最近N轮对话"""
        return self.messages[-limit:] if len(self.messages) > limit else self.messages

    def format_for_prompt(self, include_roles=True, limit=3) -> str:
        """将历史对话格式化为可用于 LLM 的提示文本"""
        history = self.get_history(limit)
        lines = []
        for role, msg in history:
            role_zh = "用户" if role == "user" else "助手"
            if include_roles:
                lines.append(f"{role_zh}：{msg}")
            else:
                lines.append(msg)
        return "\n".join(lines)

    def build_enhanced_prompt(self, question: str, schema_hint: str) -> str:
        """
        构建带上下文的增强型提示词
        :param question: 当前问题
        :param schema_hint: 数据库结构说明（来自 SchemaInspector）
        :return: 完整 prompt
        """
        context = ""
        history = self.get_history(limit=3)

        if history:
            context += "【历史对话】\n"
            for role, msg in history:
                role_zh = "用户" if role == "user" else "助手"
                context += f"{role_zh}：{msg}\n"
            context += "\n"

        # 加入知识库理解（可选）
        from core.knowledge_base import KnowledgeBase
        keywords = []
        for field, terms in KnowledgeBase.FIELD_MAPPING.items():
            for term in terms:
                if term in question:
                    actual_val = KnowledgeBase.get_actual_value(field, term)
                    if actual_val != term:
                        keywords.append(f"【{term}】→ 字段：{field}，应匹配为‘{actual_val}’")
                    else:
                        keywords.append(f"【{term}】→ 字段：{field}")

        knowledge_notes = "\n".join(keywords) if keywords else "未检测到简称。"

        full_prompt = f"""
{context}当前任务是协助用户进行充电业务数据分析。

【数据库结构】
{schema_hint}

【用户当前问题】
{question}

【上下文理解】
{knowledge_notes}

【要求】
- 准确理解指代关系（如“它”、“那”、“最近”）
- 所有字段使用真实名称，禁止虚构列名
- 使用 LIKE 实现模糊匹配
- 只返回 SELECT 查询语句本身，不要解释
- 时间条件优先使用 DATE(充电开始时间)
        """.strip()
        return full_prompt

    def clear(self):
        """清空上下文"""
        self.messages.clear()
        self.last_sql = None
        self.last_result = None

    def set_last_sql_result(self, sql: str, result: str):
        """记录最后一次查询和结果"""
        self.last_sql = sql
        self.last_result = result
