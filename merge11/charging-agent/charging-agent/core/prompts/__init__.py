# core/prompts/__init__.py
# 提示词模块初始化文件

from .sql_generator_prompt import SQL_GENERATOR_PROMPT_TEMPLATE
from .ai_responder_prompt import AI_RESPONDER_PROMPT_TEMPLATE

__all__ = [
    'SQL_GENERATOR_PROMPT_TEMPLATE',
    'AI_RESPONDER_PROMPT_TEMPLATE',
]

