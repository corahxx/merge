from langchain_community.utilities import SQLDatabase

class SchemaInspector:
    @staticmethod
    def get_schema_prompt(db: SQLDatabase) -> str:
        table_info = db.get_table_info()
        return f"""
        【数据库结构】
        {table_info}

        【注意事项】
        - 表名：evdata
        - 关键字段：充电桩编号, 充电桩类型, 充电站名称，充电站位置，运营商名称
        - 地域和运营商名称匹配必须使用 LIKE '%关键词%'，禁止使用 =
        - 时间字段注意格式转换（如 DATE(), STR_TO_DATE）
        """
