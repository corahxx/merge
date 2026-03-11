# handlers/energy_merge_handler.py - 电量表多表合并（仅合并 / 合并汇总）

import re
import os
import importlib.util
from datetime import datetime
from typing import List, Optional, Tuple, Any
import pandas as pd
from io import BytesIO

# 表头判定关键词（前3行中先出现者为表头行）
HEADER_KEYWORDS = ("省级行政区域名称", "月度充电电量")
# 七字段对应的原列关键词（列名包含即匹配）
SOURCE_COLUMN_KEYWORDS = [
    "直流桩", "交流桩", "私家车", "公交车", "出租车", "环卫物流车", "其他",
]
NEW_FIELD_NAMES = [
    "直流桩电量", "交流桩电量", "私家车使用电量", "公交车使用电量",
    "出租车使用电量", "环卫物流车使用电量", "其他使用电量",
]
MONTHLY_COL = "月度充电电量"
PROVINCE_COL = "省级行政区域名称"
MIN_NONEMPTY = 3  # 有内容 Sheet 至少非空单元格数

# 合并汇总时省级行政区域名称映射：输入包含任一关键词则映射为对应结果（按顺序匹配，先匹配先得）
PROVINCE_MAPPING = [
    (["北京市", "北京"], "北京市"),
    (["天津市", "天津"], "天津市"),
    (["上海市", "上海"], "上海市"),
    (["重庆市", "重庆"], "重庆市"),
    (["河北省", "河北"], "河北省"),
    (["山西省", "山西"], "山西省"),
    (["辽宁省", "辽宁"], "辽宁省"),
    (["吉林省", "吉林"], "吉林省"),
    (["黑龙江省", "黑龙江"], "黑龙江省"),
    (["江苏省", "江苏"], "江苏省"),
    (["浙江省", "浙江"], "浙江省"),
    (["安徽省", "安徽"], "安徽省"),
    (["福建省", "福建"], "福建省"),
    (["江西省", "江西"], "江西省"),
    (["山东省", "山东"], "山东省"),
    (["河南省", "河南"], "河南省"),
    (["湖北省", "湖北"], "湖北省"),
    (["湖南省", "湖南"], "湖南省"),
    (["广东省", "广东"], "广东省"),
    (["海南省", "海南"], "海南省"),
    (["四川省", "四川"], "四川省"),
    (["贵州省", "贵州"], "贵州省"),
    (["云南省", "云南"], "云南省"),
    (["陕西省", "陕西"], "陕西省"),
    (["甘肃省", "甘肃"], "甘肃省"),
    (["青海省", "青海"], "青海省"),
    (["内蒙古自治区", "内蒙古"], "内蒙古自治区"),
    (["广西壮族自治区", "广西"], "广西壮族自治区"),
    (["西藏自治区", "西藏"], "西藏自治区"),
    (["宁夏回族自治区", "宁夏"], "宁夏回族自治区"),
    (["新疆维吾尔自治区", "新疆"], "新疆维吾尔自治区"),
    (["香港特别行政区", "香港"], "香港特别行政区"),
    (["澳门特别行政区", "澳门"], "澳门特别行政区"),
    (["台湾省", "台湾"], "台湾省"),
]


def _map_province_name(value: Any) -> str:
    """将省级名称按 PROVINCE_MAPPING 映射；无匹配则返回原值（转字符串）。"""
    s = str(value).strip() if pd.notna(value) else ""
    if not s:
        return s
    for keywords, result in PROVINCE_MAPPING:
        for kw in keywords:
            if kw in s:
                return result
    return s


def _get_operator_name(file_name: str) -> str:
    """根据文件名取运营商名称，无匹配返回「未识别」。"""
    try:
        _dir = os.path.dirname(os.path.abspath(__file__))
        _path = os.path.join(_dir, "operator_name_rules.py")
        _spec = importlib.util.spec_from_file_location("operator_name_rules", _path)
        if _spec and _spec.loader:
            _mod = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)
            s = _mod.get_operator_name_from_table_name(file_name or "")
            return s if s else "未识别"
    except Exception:
        pass
    return "未识别"


def _row_contains_any(row_series: pd.Series, *keywords: str) -> bool:
    text = " ".join(str(v) for v in row_series.astype(str).tolist())
    return any(kw in text for kw in keywords)


def _sheet_has_content(file_bytes: bytes, sheet_name: str, engine: str) -> bool:
    try:
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=None, nrows=5, engine=engine)
        return int(df.notna().sum().sum()) >= MIN_NONEMPTY
    except Exception:
        return False


def _find_header_row(rows: List[pd.Series], table_name: str) -> Tuple[Optional[int], Optional[str]]:
    """前3行中先出现 HEADER_KEYWORDS 的行作为表头，返回 (0-based 行号, None) 或 (None, 错误信息)。"""
    for i in range(min(3, len(rows))):
        if _row_contains_any(rows[i], *HEADER_KEYWORDS):
            return i, None
    return None, f"「{table_name}」无法确定表头（前3行未出现「省级行政区域名称」或「月度充电电量」）"


def _parse_number_for_ratio(val: Any) -> Optional[float]:
    """解析数值，百分数转为小数（如 30% -> 0.3）。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _clean_numeric_cell(val: Any) -> Any:
    """去掉单位（KWh、KWH 等）和代表空值的字符（-、/），返回 float 或 pd.NA。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return pd.NA
    s = str(val).strip()
    if not s or s in ("-", "/") or all(c in "-/ \t" for c in s):
        return pd.NA
    # 去掉单位（不区分大小写）
    s = re.sub(r"\s*\(?\s*kwh\s*\)?\s*", "", s, flags=re.I)
    s = re.sub(r"\s*kwh\s*", "", s, flags=re.I)
    s = s.replace("-", "").replace("/", "").strip()
    if not s:
        return pd.NA
    # 百分数去掉 % 后转数值（如 30% -> 30）
    s = s.replace("%", "").strip()
    if not s:
        return pd.NA
    try:
        return float(s)
    except ValueError:
        return pd.NA


def _is_ratio_column(series: pd.Series) -> bool:
    """该列是否视为比例：有百分数（任一含%）或全部数值型单元格的值 ≤100。"""
    # 任一单元格含 % 则视为比例
    for v in series:
        if pd.notna(v) and "%" in str(v).strip():
            return True
    # 否则看全部数值型：若全部 ≤100 则视为比例
    vals = []
    for v in series:
        n = _parse_number_for_ratio(v)
        if n is None:
            continue  # 忽略非数值，只判数值型
        vals.append(n)
    if not vals:
        return True  # 无数值单元格时保守视为比例
    return all(x <= 100 for x in vals)


def _add_seven_fields(df: pd.DataFrame) -> pd.DataFrame:
    """按规则 5.2 新增七个字段；参与计算的列先做数值清洗（去单位、去空值字符）。"""
    df = df.copy()
    # 查找 月度充电电量 列
    month_col = None
    for c in df.columns:
        if MONTHLY_COL in str(c):
            month_col = c
            break
    if month_col is None:
        for c in df.columns:
            if "月度" in str(c) and "电量" in str(c):
                month_col = c
                break
    # 月度充电电量列先清洗为数值，并写回原列，使最终输出也为纯数值（无 KWh、-、/）
    monthly = df[month_col].apply(_clean_numeric_cell) if month_col is not None else pd.Series(dtype=float)
    if month_col is not None:
        df[month_col] = monthly.fillna(0)
    # 为每个新字段找源列
    for i, keyword in enumerate(SOURCE_COLUMN_KEYWORDS):
        src_col = None
        for c in df.columns:
            if keyword in str(c):
                src_col = c
                break
        new_name = NEW_FIELD_NAMES[i]
        if src_col is None:
            df[new_name] = pd.NA
            continue
        # 源列先清洗为数值（去单位、去 - / 等空值字符），并写回原列，使最终输出也为纯数值
        series_clean = df[src_col].apply(_clean_numeric_cell)
        df[src_col] = series_clean.fillna(0)
        if month_col is not None and _is_ratio_column(series_clean.dropna()):
            raw = series_clean
            # 若 raw 已在 [0,1] 则视为已为小数，否则视为百分数除以 100
            valid = raw.dropna()
            if len(valid) > 0 and valid.max() <= 1:
                df[new_name] = monthly * raw
            else:
                df[new_name] = monthly * raw / 100.0
        else:
            df[new_name] = series_clean
    # 七字段空值填充为 0
    for col in NEW_FIELD_NAMES:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    return df


def _select_sheet(file_bytes: bytes, engine: str, file_name: str) -> Tuple[Optional[str], Optional[str]]:
    """返回 (sheet_name, error)。若多个有内容：sheet1/sheet2 形式用 sheet1；日期命名用最新；否则第一个有内容。"""
    try:
        xl = pd.ExcelFile(BytesIO(file_bytes), engine=engine)
    except Exception as e:
        return None, f"打开工作簿失败: {e}"
    names = xl.sheet_names
    if not names:
        return None, "工作簿无工作表"
    content_sheets = [n for n in names if _sheet_has_content(file_bytes, n, engine)]
    if not content_sheets:
        return None, "未找到有内容的 Sheet"
    if len(content_sheets) == 1:
        return content_sheets[0], None
    # 多个：是否均为 sheet1, sheet2 形式
    pattern = re.compile(r"^sheet\s*\d+$", re.I)
    if all(pattern.match(str(n).strip()) for n in content_sheets):
        for n in content_sheets:
            if re.match(r"^sheet\s*1\s*$", str(n).strip(), re.I):
                return n, None
        return content_sheets[0], None
    # 尝试按日期解析
    dates = []
    for n in content_sheets:
        m = re.search(r"(\d{4})[-/]?(\d{1,2})", str(n))
        if m:
            try:
                y, mo = int(m.group(1)), int(m.group(2))
                if 1 <= mo <= 12:
                    dates.append((datetime(y, mo, 1), n))
            except ValueError:
                pass
    if dates:
        dates.sort(key=lambda x: x[0], reverse=True)
        return dates[0][1], None
    return content_sheets[0], None


def process_one_file(
    file_bytes: bytes,
    file_name: str,
    engine: str = "openpyxl",
) -> Tuple[Optional[pd.DataFrame], Optional[str], List[str]]:
    """处理单个文件，返回 (df, error, warnings)。df 已含 文件名称、运营商名称、七字段。"""
    errors: List[str] = []
    ext = file_name[file_name.rfind(".") :].lower() if "." in file_name else ""
    if ext == ".csv":
        try:
            df = pd.read_csv(BytesIO(file_bytes), header=None, nrows=3, encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(BytesIO(file_bytes), header=None, nrows=3, encoding="gbk")
        rows = [df.iloc[i] for i in range(min(3, len(df)))]
        header_row, err = _find_header_row(rows, file_name)
        if err:
            return None, err, errors
        try:
            df = pd.read_csv(BytesIO(file_bytes), header=header_row, encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(BytesIO(file_bytes), header=header_row, encoding="gbk")
    else:
        if ext not in (".xlsx", ".xls"):
            return None, f"「{file_name}」不支持的文件格式", errors
        if ext == ".xls":
            engine = "xlrd"
        sheet_name, err = _select_sheet(file_bytes, engine, file_name)
        if err:
            return None, f"「{file_name}」{err}", errors
        try:
            df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=None, nrows=10, engine=engine)
        except Exception:
            df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=None, nrows=10, engine=engine)
        rows = [df.iloc[i] for i in range(min(3, len(df)))]
        table_name = f"{file_name}（{sheet_name}）"
        header_row, err = _find_header_row(rows, table_name)
        if err:
            return None, err, errors
        try:
            df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=header_row, engine=engine)
        except Exception as e:
            return None, f"「{table_name}」读表失败: {e}", errors
    if df.empty:
        return None, f"「{file_name}」表头解析后无数据", errors
    df.insert(0, "文件名称", file_name)
    df.insert(1, "运营商名称", _get_operator_name(file_name))
    df = _add_seven_fields(df)
    return df, None, errors


def merge_only(
    files: List[Tuple[str, bytes]],
    engine: str = "openpyxl",
) -> Tuple[Optional[pd.DataFrame], List[str], List[str], List[Tuple[str, int]]]:
    """仅合并：各文件处理后纵向拼接。返回 (merged_df, success_list, error_list, row_counts)。"""
    merged: List[pd.DataFrame] = []
    success: List[str] = []
    errors: List[str] = []
    row_counts: List[Tuple[str, int]] = []
    for name, b in files:
        try:
            df, err, _ = process_one_file(b, name, engine)
        except Exception as e:
            errors.append(f"「{name}」合并时出错: {e}")
            continue
        if err:
            errors.append(err)
            continue
        if df is not None and len(df) > 0:
            merged.append(df)
            success.append(name)
            row_counts.append((name, len(df)))
    if not merged:
        return None, success, errors, []
    return pd.concat(merged, ignore_index=True), success, errors, row_counts


def merge_aggregate(
    files: List[Tuple[str, bytes]],
    engine: str = "openpyxl",
) -> Tuple[Optional[pd.DataFrame], List[str], List[str], List[Tuple[str, int]]]:
    """合并汇总：各文件处理后拼接，再按省级行政区域名称加总；输出表必须包含月度充电电量及七项电量。"""
    merged_df, success, errors, row_counts = merge_only(files, engine)
    if merged_df is None or merged_df.empty:
        return None, success, errors, row_counts
    province_col = None
    for c in merged_df.columns:
        if PROVINCE_COL in str(c):
            province_col = c
            break
    if province_col is None:
        return merged_df, success, errors, row_counts
    # 月度充电电量：按列名包含关键词查找，确保输出表包含该列
    month_col = None
    for c in merged_df.columns:
        if MONTHLY_COL in str(c):
            month_col = c
            break
    if month_col is None:
        for c in merged_df.columns:
            if "月度" in str(c) and "电量" in str(c):
                month_col = c
                break
    sum_cols = []
    if month_col is not None:
        sum_cols.append(month_col)
    for c in NEW_FIELD_NAMES:
        if c in merged_df.columns:
            sum_cols.append(c)
    if not sum_cols:
        return merged_df, success, errors, row_counts
    merged_df = merged_df.copy()
    # 省级名称映射
    merged_df[province_col] = merged_df[province_col].apply(_map_province_name)
    # 排除「合计」「总计」行
    merged_df = merged_df[
        ~merged_df[province_col].astype(str).str.strip().isin(["合计", "总计"])
    ]
    # 确保分组列为字符串，避免 groupby 时类型混合
    merged_df[province_col] = merged_df[province_col].astype(str)
    # 确保加总列为数值，避免 sum 时 str+float 报错（先做数值清洗再去空）
    for col in sum_cols:
        merged_df[col] = merged_df[col].apply(_clean_numeric_cell)
        merged_df[col] = merged_df[col].fillna(0)
    agg_df = merged_df.groupby(province_col, as_index=False)[sum_cols].sum()
    return agg_df, success, errors, row_counts
