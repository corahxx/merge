# handlers/generic_merge_handler.py - 其他类型表格合并（纵向/横向）

from typing import List, Optional, Tuple
import pandas as pd
from io import BytesIO

MIN_NONEMPTY = 3


def _sheet_has_content(file_bytes: bytes, sheet_name: str, engine: str) -> bool:
    try:
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=None, nrows=5, engine=engine)
        return int(df.notna().sum().sum()) >= MIN_NONEMPTY
    except Exception:
        return False


def _first_sheet_name(file_bytes: bytes, engine: str):
    """取第一个有内容的 Sheet 名称；若无则 None。"""
    try:
        xl = pd.ExcelFile(BytesIO(file_bytes), engine=engine)
        for n in xl.sheet_names:
            if _sheet_has_content(file_bytes, n, engine):
                return n
    except Exception:
        pass
    return None


def read_one_table(
    file_bytes: bytes,
    file_name: str,
    engine: str = "openpyxl",
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """读取单表：首行为表头，CSV 或 Excel 第一个有内容 Sheet。返回 (df, error)。"""
    ext = file_name[file_name.rfind(".") :].lower() if "." in file_name else ""
    if ext == ".csv":
        try:
            df = pd.read_csv(BytesIO(file_bytes), header=0, encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(BytesIO(file_bytes), header=0, encoding="gbk")
    elif ext in (".xlsx", ".xls"):
        if ext == ".xls":
            engine = "xlrd"
        sheet = _first_sheet_name(file_bytes, engine)
        if sheet is None:
            return None, f"「{file_name}」未找到有内容的 Sheet"
        try:
            df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet, header=0, engine=engine)
        except Exception as e:
            return None, f"「{file_name}」读表失败: {e}"
    else:
        return None, f"「{file_name}」不支持的文件格式"
    if df is None or df.empty:
        return None, f"「{file_name}」表为空"
    return df, None


def get_columns_from_files(
    files: List[Tuple[str, bytes]],
    engine: str = "openpyxl",
) -> Tuple[List[str], List[str], List[str]]:
    """从已上传文件中解析字段列表（以第一张成功读取的表头为准）。返回 (columns, success_names, errors)。"""
    columns = []
    success = []
    errors = []
    for name, b in files:
        df, err = read_one_table(b, name, engine)
        if err:
            errors.append(err)
            continue
        success.append(name)
        if not columns and df is not None:
            columns = list(df.columns.astype(str))
        break
    return columns, success, errors


def _dedup_table_names(names: List[str]) -> List[str]:
    """表名称去重：去掉公共前缀与公共后缀，剩余作为列名；若为空则用原名称。"""
    if not names:
        return []
    names = [str(n).strip() for n in names]
    originals = list(names)
    if len(names) == 1:
        return names
    prefix = names[0]
    for s in names[1:]:
        while prefix and not s.startswith(prefix):
            prefix = prefix[:-1]
    suffix = names[0]
    for s in names[1:]:
        while suffix and not s.endswith(suffix):
            suffix = suffix[1:]
    result = []
    for i, s in enumerate(names):
        if prefix and s.startswith(prefix):
            s = s[len(prefix) :]
        if suffix and s.endswith(suffix):
            s = s[: -len(suffix)] if len(suffix) > 0 else s
        s = s.strip()
        if not s and i < len(originals):
            s = originals[i]
        result.append(s or originals[i] if i < len(originals) else names[-1])
    return result


def merge_vertical(
    files: List[Tuple[str, bytes]],
    selected_columns: List[str],
    engine: str = "openpyxl",
) -> Tuple[Optional[pd.DataFrame], List[str], List[str]]:
    """纵向合并：每表取 selected_columns，最前加「表名称」列后纵向拼接。返回 (df, success_list, error_list)。"""
    if not selected_columns:
        return None, [], ["请至少选择一列纵向合并字段"]
    merged = []
    success = []
    errors = []
    for name, b in files:
        df, err = read_one_table(b, name, engine)
        if err:
            errors.append(err)
            continue
        if df is None:
            continue
        n = len(df)
        sub = pd.DataFrame({"表名称": [name] * n})
        for col in selected_columns:
            if col in df.columns:
                sub[col] = df[col].values
            else:
                sub[col] = pd.NA
        merged.append(sub)
        success.append(name)
    if not merged:
        return None, success, errors
    return pd.concat(merged, ignore_index=True), success, errors


def merge_horizontal(
    files: List[Tuple[str, bytes]],
    align_col: str,
    merge_columns: List[str],
    column_name_mode: str,
    with_aggregate: bool,
    engine: str = "openpyxl",
) -> Tuple[Optional[pd.DataFrame], List[str], List[str]]:
    """横向合并：按 align_col 对齐，各表 merge_columns 以新列追加；column_name_mode 为 表名称 或 表名称去重；with_aggregate 则最后加一列汇总（求和）。"""
    if not align_col or not merge_columns:
        return None, [], ["请选择横向对齐字段和横向合并字段"]
    tables = []
    success = []
    errors = []
    for name, b in files:
        df, err = read_one_table(b, name, engine)
        if err:
            errors.append(err)
            continue
        if df is None or align_col not in df.columns:
            errors.append(f"「{name}」缺少对齐列「{align_col}」")
            continue
        tables.append((name, df))
        success.append(name)
    if not tables:
        return None, success, errors

    names = [t[0] for t in tables]
    if column_name_mode == "表名称去重":
        display_names = _dedup_table_names(names)
    else:
        display_names = names

    base_name, base_df = tables[0]
    keys = base_df[align_col].astype(str).drop_duplicates().tolist()
    result = base_df[[align_col]].drop_duplicates(subset=[align_col]).copy()
    result = result.reset_index(drop=True)

    numeric_cols_for_agg = []
    for idx, (name, df) in enumerate(tables):
        prefix = display_names[idx] if idx < len(display_names) else name
        key_series = df[align_col].astype(str)
        for col in merge_columns:
            if col not in df.columns:
                result[f"{prefix}_{col}"] = pd.NA
                continue
            new_name = f"{prefix}_{col}"
            val_map = dict(zip(key_series, df[col]))
            result[new_name] = result[align_col].astype(str).map(val_map)
            if pd.api.types.is_numeric_dtype(df[col]):
                numeric_cols_for_agg.append(new_name)

    if with_aggregate and numeric_cols_for_agg:
        result["汇总"] = result[numeric_cols_for_agg].apply(pd.to_numeric, errors="coerce").sum(axis=1)

    return result, success, errors


def _to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def run_validation(df: pd.DataFrame, rules: List[dict]) -> List[dict]:
    """对 df 按行执行规则校验。rules 每项为 dict，含 left_field, left_op, left_right_type, left_right_field/left_right_constant, relation, right_field/right_constant, tolerance。返回 list of { label, passed, violation_count, violation_indices, violation_df }。"""
    if df is None or df.empty or not rules:
        return []
    results = []
    for i, r in enumerate(rules):
        left_field = r.get("left_field")
        relation = r.get("relation", "=")
        if not left_field or left_field not in df.columns:
            results.append({"label": f"规则{i+1}（左侧字段缺失）", "passed": False, "violation_count": len(df), "violation_indices": list(range(len(df))), "violation_df": df.head(0)})
            continue
        left_val = _to_numeric_series(df[left_field])
        left_op = r.get("left_op")
        if left_op:
            right_type = r.get("left_right_type", "constant")
            if right_type == "field":
                rf = r.get("left_right_field")
                right_operand = _to_numeric_series(df[rf]) if rf and rf in df.columns else pd.Series([pd.NA] * len(df))
            else:
                c = r.get("left_right_constant")
                right_operand = pd.Series([float(c)] * len(df)) if c is not None and not (isinstance(c, float) and pd.isna(c)) else pd.Series([pd.NA] * len(df))
            if left_op == "+": left_val = left_val + right_operand
            elif left_op == "-": left_val = left_val - right_operand
            elif left_op == "*": left_val = left_val * right_operand
            elif left_op == "/": left_val = left_val / right_operand.replace(0, pd.NA)
        right_field = r.get("right_field")
        if right_field and str(right_field) in df.columns:
            right_val = _to_numeric_series(df[right_field])
        else:
            c = r.get("right_constant")
            right_val = pd.Series([float(c)] * len(df)) if c is not None and not (isinstance(c, float) and pd.isna(c)) else pd.Series([pd.NA] * len(df))
        tol = r.get("tolerance") if r.get("tolerance") is not None else 1e-9
        violation_mask = pd.Series([False] * len(df), index=df.index)
        for idx in df.index:
            lv, rv = left_val.at[idx], right_val.at[idx]
            if pd.isna(lv) or pd.isna(rv):
                violation_mask.at[idx] = True
                continue
            lf, rf = float(lv), float(rv)
            if relation == "=": violation_mask.at[idx] = abs(lf - rf) > tol
            elif relation == ">": violation_mask.at[idx] = not (lf > rf)
            elif relation == ">=": violation_mask.at[idx] = not (lf >= rf)
            elif relation == "<": violation_mask.at[idx] = not (lf < rf)
            elif relation == "<=": violation_mask.at[idx] = not (lf <= rf)
            elif relation == "!=": violation_mask.at[idx] = abs(lf - rf) <= tol
            else: violation_mask.at[idx] = True
        violation_indices = df.index[violation_mask].tolist()
        violation_df = df.loc[violation_mask] if violation_mask.any() else df.head(0)
        results.append({"label": r.get("label") or f"规则{i+1}", "passed": len(violation_indices) == 0, "violation_count": len(violation_indices), "violation_indices": violation_indices, "violation_df": violation_df})
    return results
