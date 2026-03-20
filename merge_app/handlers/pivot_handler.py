# handlers/pivot_handler.py - 数据透视表（仿 Excel 透视）

import re
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd

# 聚合方式与 pandas 的对应关系
AGG_MAP = {
    "加总": "sum",
    "计数": "count",
    "计数非空": "count",
    "平均值": "mean",
    "最小值": "min",
    "最大值": "max",
    "首次": "first",
    "末次": "last",
}


def get_numeric_columns(df: pd.DataFrame) -> List[str]:
    """返回数值型列名列表，用于推荐值字段。"""
    cols = []
    for c in df.columns:
        try:
            if pd.api.types.is_numeric_dtype(df[c]):
                cols.append(c)
        except Exception:
            pass
    return cols


def build_pivot_table(
    df: pd.DataFrame,
    index_cols: List[str],
    columns_cols: List[str],
    values_aggs: List[Tuple[str, str]],
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    根据行、列、值及聚合方式生成透视表。
    index_cols: 行维度列名列表；columns_cols: 列维度列名列表；values_aggs: [(值列名, 聚合中文名)], 聚合见 AGG_MAP。
    返回 (透视后 DataFrame, 错误信息)。成功时 error 为 None。
    """
    if df is None or df.empty:
        return None, "没有可用的数据。"
    if not index_cols and not columns_cols:
        return None, "请至少选择行字段或列字段。"
    if not values_aggs:
        return None, "请至少添加一个值字段及聚合方式。"

    # 检查列存在
    all_cols = set(df.columns)
    for c in index_cols + columns_cols:
        if c not in all_cols:
            return None, f"列不存在：{c}"
    for v, _ in values_aggs:
        if v not in all_cols:
            return None, f"值列不存在：{v}"

    aggfunc_map = {}
    for v, agg_name in values_aggs:
        pandas_agg = AGG_MAP.get(agg_name)
        if pandas_agg is None:
            return None, f"不支持的聚合方式：{agg_name}"
        if v not in aggfunc_map:
            aggfunc_map[v] = pandas_agg
        else:
            # 同一列多种聚合：pandas 支持 list，如 [sum, count]
            if isinstance(aggfunc_map[v], list):
                aggfunc_map[v].append(pandas_agg)
            else:
                aggfunc_map[v] = [aggfunc_map[v], pandas_agg]

    try:
        index = list(index_cols) if index_cols else []
        columns = list(columns_cols) if columns_cols else None
        values = list({v for v, _ in values_aggs})
        use_dummy_index = not index
        if use_dummy_index:
            df = df.copy()
            df["__pivot_index__"] = 1
            index = ["__pivot_index__"]
        result = pd.pivot_table(
            df,
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc_map,
            fill_value=None,
        )
        if use_dummy_index:
            result = result.reset_index(drop=True)
        return result, None
    except Exception as e:
        return None, f"生成透视表失败：{e}"


def filter_dataframe(
    df: pd.DataFrame,
    filter_col: Optional[str] = None,
    selected_values: Optional[List[Any]] = None,
    where_expr: Optional[str] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    对 DataFrame 做筛选。二选一：
    - 按取值：filter_col + selected_values，保留 filter_col 在 selected_values 中的行。
    - 按条件：where_expr，使用 pandas 的 query 语法（Python 表达式，如 age > 18 and city == '北京'）。
    返回 (筛选后 df, 错误信息)。
    """
    if df is None or df.empty:
        return None, "没有可用的数据。"
    if selected_values is not None:
        if not filter_col or filter_col not in df.columns:
            return None, "筛选字段不存在或未选择。"
        out = df[df[filter_col].isin(selected_values)].copy()
        return out, None
    if where_expr and where_expr.strip():
        try:
            # pandas query：列名若含空格等需用反引号
            out = df.query(where_expr.strip())
            return out, None
        except Exception as e:
            return None, f"筛选条件执行失败：{e}"
    return df, None


def get_distinct_values(df: pd.DataFrame, column: str, limit: int = 10000) -> Tuple[Optional[List[Any]], Optional[str]]:
    """返回某列去重后的值列表（用于「按取值选择」）。超过 limit 时截断。"""
    if df is None or column not in df.columns:
        return None, "列不存在。"
    try:
        vals = df[column].dropna().unique().tolist()
        if len(vals) > limit:
            vals = vals[:limit]
        # 统一转为可比较/可显示类型
        vals = [v if pd.notna(v) else None for v in vals]
        return vals, None
    except Exception as e:
        return None, str(e)


# ---------- 数据库支持（MySQL / PostgreSQL） ----------
DB_TYPE_MYSQL = "mysql"
DB_TYPE_PSQL = "psql"


def _get_connection_mysql(host: str, port: Union[int, str], user: str, password: str, database: str):
    try:
        import pymysql
        port = int(port) if port is not None else 3306
        return pymysql.connect(
            host=host.strip(),
            port=port,
            user=user.strip(),
            password=password,
            database=database.strip(),
            charset="utf8mb4",
        )
    except ImportError:
        raise RuntimeError("请安装 pymysql：pip install pymysql")
    except Exception as e:
        raise RuntimeError(f"连接数据库失败：{e}")


def _get_connection_psql(host: str, port: Union[int, str], user: str, password: str, database: str):
    try:
        import psycopg2
        port = int(port) if port is not None else 5432
        return psycopg2.connect(
            host=host.strip(),
            port=port,
            user=user.strip(),
            password=password,
            dbname=database.strip(),
        )
    except ImportError:
        raise RuntimeError("请安装 psycopg2：pip install psycopg2-binary")
    except Exception as e:
        raise RuntimeError(f"连接数据库失败：{e}")


def _get_connection(db_type: str, host: str, port: Union[int, str], user: str, password: str, database: str):
    if db_type == DB_TYPE_PSQL:
        return _get_connection_psql(host, port, user, password, database)
    return _get_connection_mysql(host, port, user, password, database)


def _sql_ident_mysql(s: str) -> str:
    return "`" + s.replace("`", "``") + "`"


def _sql_ident_psql(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def get_db_columns(
    db_type: str,
    host: str,
    port: Union[int, str],
    user: str,
    password: str,
    database: str,
    table: str,
    schema: Optional[str] = None,
) -> Tuple[Optional[List[str]], Optional[str]]:
    """连接数据库，返回指定表的列名列表。db_type: mysql | psql。psql 时 schema 默认 public。"""
    try:
        conn = _get_connection(db_type, host, port, user, password, database)
        try:
            if db_type == DB_TYPE_PSQL:
                schema = (schema or "public").strip()
                q = 'SELECT * FROM "%s"."%s" LIMIT 0' % (schema.replace('"', '""'), table.replace('"', '""'))
            else:
                q = "SELECT * FROM `%s` LIMIT 0" % table.replace("`", "``")
            df = pd.read_sql(q, conn)
            return list(df.columns), None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)


def load_table_from_db(
    db_type: str,
    host: str,
    port: Union[int, str],
    user: str,
    password: str,
    database: str,
    table: str,
    schema: Optional[str] = None,
    limit: int = 1_000_000,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    从数据库表加载数据为 DataFrame，供数据清洗等使用。
    limit: 最大行数，避免大表一次性拉取导致 OOM。返回 (df, 错误信息)。
    """
    try:
        conn = _get_connection(db_type, host, port, user, password, database)
        try:
            if db_type == DB_TYPE_PSQL:
                ident = _sql_ident_psql
                schema = (schema or "public").strip()
                tbl = "%s.%s" % (ident(schema), ident(table))
            else:
                tbl = _sql_ident_mysql(table)
            q = "SELECT * FROM %s LIMIT %d" % (tbl, max(1, limit))
            df = pd.read_sql(q, conn)
            return df, None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)


def get_db_distinct_values(
    db_type: str,
    host: str,
    port: Union[int, str],
    user: str,
    password: str,
    database: str,
    table: str,
    column: str,
    limit: int = 10000,
    schema: Optional[str] = None,
) -> Tuple[Optional[List[Any]], Optional[str]]:
    """返回表中某列的去重值，用于「按取值选择」筛选。"""
    try:
        conn = _get_connection(db_type, host, port, user, password, database)
        try:
            if db_type == DB_TYPE_PSQL:
                ident = _sql_ident_psql
                schema = (schema or "public").strip()
                tbl = '%s.%s' % (ident(schema), ident(table))
                col_esc = ident(column)
                sql = 'SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL LIMIT %s' % (col_esc, tbl, col_esc, limit)
            else:
                col_esc = _sql_ident_mysql(column)
                tbl_esc = _sql_ident_mysql(table)
                sql = "SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL LIMIT %s" % (col_esc, tbl_esc, col_esc, limit)
            with conn.cursor() as cur:
                cur.execute(sql)
                vals = [row[0] for row in cur.fetchall()]
            return vals, None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)


def build_pivot_from_db(
    db_type: str,
    host: str,
    port: Union[int, str],
    user: str,
    password: str,
    database: str,
    table: str,
    index_cols: List[str],
    columns_cols: List[str],
    values_aggs: List[Tuple[str, str]],
    where_clause: Optional[str] = None,
    schema: Optional[str] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    在数据库侧做聚合，返回透视结果 DataFrame。
    db_type: mysql | psql。where_clause: 可选 WHERE 条件（不含 WHERE 关键字）。
    """
    if not index_cols and not columns_cols:
        return None, "请至少选择行字段或列字段。"
    if not values_aggs:
        return None, "请至少添加一个值字段及聚合方式。"
    sql_agg = {"加总": "SUM", "计数": "COUNT", "计数非空": "COUNT", "平均值": "AVG", "最小值": "MIN", "最大值": "MAX"}
    if db_type == DB_TYPE_PSQL:
        ident = _sql_ident_psql
        schema = (schema or "public").strip()
        tbl = "%s.%s" % (ident(schema), ident(table))
    else:
        ident = _sql_ident_mysql
        tbl = ident(table)
    select_parts = []
    group_parts = []
    for c in index_cols + columns_cols:
        select_parts.append(ident(c))
        group_parts.append(ident(c))
    for v, agg_name in values_aggs:
        sql_fn = sql_agg.get(agg_name)
        if not sql_fn:
            return None, f"数据库暂不支持聚合：{agg_name}"
        alias = re.sub(r"\W+", "_", f"{v}_{agg_name}")[:64] or "agg"
        if db_type == DB_TYPE_PSQL:
            select_parts.append('%s(%s) AS %s' % (sql_fn, ident(v), ident(alias)))
        else:
            select_parts.append("%s(%s) AS `%s`" % (sql_fn, ident(v), alias.replace("`", "")))
    sql = "SELECT " + ", ".join(select_parts) + " FROM " + tbl
    if where_clause and where_clause.strip():
        sql += " WHERE " + where_clause.strip()
    sql += " GROUP BY " + ", ".join(group_parts)
    try:
        conn = _get_connection(db_type, host, port, user, password, database)
        try:
            df = pd.read_sql(sql, conn)
            return df, None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)
