class QueryExecutor:
    def __init__(self, db):
        self.db = db

    def run(self, sql: str):
        try:
            result = self.db.run(sql)
            return result
        except Exception as e:
            raise RuntimeError(f"SQL执行失败：{e}\n请检查语法或权限。")
