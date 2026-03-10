# core/redirector.py
from core.knowledge_base import KnowledgeBase

class Redirector:
    def __init__(self, context):
        self.context = context
        self.follow_up_words = ['呢', '如何', '怎么样', '情况', '好吗', '有没有', '咋样', '表现']
        # 使用统一的知识库
        self.known_operators = KnowledgeBase.OPERATOR_NICKNAMES
        self.locations = KnowledgeBase.LOCATION_NICKNAMES

    def should_redirect(self, question: str) -> bool:
        is_follow = any(w in question for w in self.follow_up_words)
        return is_follow and self.context.has_history()

    def extract_entity(self, text: str, mapping: dict):
        for abbr, full in mapping.items():
            if abbr in text:
                return full
        return None

    def build_query(self, current_q: str) -> str | None:
        cur_op = self.extract_entity(current_q, self.known_operators)
        cur_loc = self.extract_entity(current_q, self.locations)

        last_op, last_loc = None, None
        for q, _ in reversed(self.context.history):
            if not last_op:
                op = self.extract_entity(q, self.known_operators)
                if op:
                    last_op = op
            if not last_loc:
                loc = self.extract_entity(q, self.locations)
                if loc:
                    last_loc = loc
            if last_op and last_loc:
                break

        final_op = cur_op or last_op
        final_loc = cur_loc or last_loc

        if not (final_op or final_loc):
            return None

        parts = ["帮我查一下"]
        if final_op: parts.append(final_op)
        if final_loc: parts.append(final_loc)
        parts.append("的充电桩数量")
        return " ".join(parts)
