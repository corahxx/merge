# core/context_manager.py
class ContextManager:
    def __init__(self, max_len=10, verbose=False):
        self._history = []
        self.max_len = max_len  # ✅ 保存为实例变量
        self.verbose = verbose

    def add_interaction(self, q, a):
        """添加一次对话交互"""
        self._history.append((q, a))
        if len(self._history) > self.max_len:  # ✅ 使用 self.max_len
            self._history.pop(0)

    def get_last_few_questions(self, n=3):
        """获取最近n条用户提问"""
        return [item[0] for item in self._history[-n:]]

    def has_history(self):
        """是否有历史记录"""
        return len(self._history) > 0

    @property
    def history(self):
        """只读访问历史"""
        return self._history.copy()

    def clear(self):
        """清空历史"""
        self._history.clear()
