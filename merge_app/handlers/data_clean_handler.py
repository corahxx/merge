# handlers/data_clean_handler.py - 充电数据清洗与标准化（规范 V2.0）

import re
import pandas as pd
from typing import Optional, Tuple, Dict, List, Any, Set, Iterable
from datetime import datetime

# 规则 ID（用于自定义清洗勾选与 applied_rules 报告）
RULE_NULL_STD = "null_std"
RULE_SEQUENCE = "sequence"
RULE_LOCATION = "location"
RULE_DATE = "date"
RULE_POWER = "power"
RULE_VOLTAGE = "voltage"
RULE_CURRENT = "current"
RULE_STATION_INNER_ID = "station_inner_id"
RULE_PILE_DEVICE_TYPE = "pile_device_type"
RULE_PILE_OPEN_TIME = "pile_open_time"

RULE_LABELS = {
    RULE_NULL_STD: "空值标准化",
    RULE_SEQUENCE: "主键生成（序号）",
    RULE_LOCATION: "充电站位置截断（≤600字）",
    RULE_DATE: "日期清洗（yyyy/mm/dd + 结果列）",
    RULE_POWER: "功率→kW",
    RULE_VOLTAGE: "电压→V",
    RULE_CURRENT: "电流→A",
    RULE_STATION_INNER_ID: "充电站内部编号缺失校验",
    RULE_PILE_DEVICE_TYPE: "设备类型标准化（交流/直流）",
    RULE_PILE_OPEN_TIME: "设备开通时间校验",
}

# 按表类型适用的规则（执行顺序）
RULES_STATION = [
    RULE_NULL_STD, RULE_SEQUENCE, RULE_LOCATION, RULE_DATE,
    RULE_POWER, RULE_VOLTAGE, RULE_CURRENT, RULE_STATION_INNER_ID,
]
RULES_PILE = [
    RULE_NULL_STD, RULE_SEQUENCE, RULE_LOCATION, RULE_DATE,
    RULE_POWER, RULE_VOLTAGE, RULE_CURRENT,
    RULE_PILE_DEVICE_TYPE, RULE_PILE_OPEN_TIME,
]


def get_rules_for_table_type(table_type: str) -> List[Tuple[str, str]]:
    """返回当前表类型适用的规则列表 [(rule_id, 中文名), ...]，供 UI 展示与勾选。"""
    if table_type == "station":
        ids = RULES_STATION
    else:
        ids = RULES_PILE
    return [(rid, RULE_LABELS.get(rid, rid)) for rid in ids]


# 功率相关列（kW 换算）
POWER_COLUMNS = [
    "站点总装机功率", "交流桩总装机功率", "直流桩总装机功率", "额定功率",
]
# 电压相关列（V）
VOLTAGE_COLUMNS = ["额定电压上限", "额定电压下限"]
# 电流相关列（A）
CURRENT_COLUMNS = ["额定电流上限", "额定电流下限"]
# 常见日期列
DATE_LIKE_KEYWORDS = ("时间", "日期", "投入使用", "开通", "生产", "入库")
LOCATION_MAX_LEN = 600
STATION_INNER_ID_COL = "充电站内部编号"
LOCATION_COL = "充电站位置"
OPEN_TIME_COL = "设备开通时间"
DEVICE_TYPE_COL = "充电桩类型"


def _detect_table_type(df: pd.DataFrame) -> str:
    """根据列名判断是充电站表还是充电桩表。"""
    cols = set(df.columns)
    if "站点总装机功率" in cols or ("充电站内部编号" in cols and "充电桩编号" not in cols):
        return "station"
    if "充电桩编号" in cols or "额定功率" in cols or "设备开通时间" in cols:
        return "pile"
    return "station"  # 默认按站表处理


def _standardize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """空值标准化：null、NULL、文本 'null' -> ''。"""
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).replace(
                ["null", "NULL", "None", "nan", "NaN"], ""
            )
            # 保留 pandas 的 NaN 转为空字符串的语义
            df[c] = df[c].replace("nan", "")
    return df


def _ensure_sequence_column(df: pd.DataFrame) -> pd.DataFrame:
    """生成从 1 递增的序号列，若已有则覆盖。"""
    df = df.copy()
    if "序号" in df.columns:
        df = df.drop(columns=["序号"])
    df.insert(0, "序号", range(1, len(df) + 1))
    return df


def _truncate_location(df: pd.DataFrame) -> pd.DataFrame:
    """充电站位置超过 600 字符截断。"""
    if LOCATION_COL not in df.columns:
        return df
    df = df.copy()
    col = df[LOCATION_COL].astype(str)
    mask = col.str.len() > LOCATION_MAX_LEN
    df.loc[mask, LOCATION_COL] = col[mask].str[:LOCATION_MAX_LEN]
    return df


def _parse_date_to_ymd(val: Any) -> Tuple[Optional[str], bool]:
    """
    尝试将单个值解析为 yyyy/mm/dd。缺失日补 1。
    返回 (标准化日期字符串 或 None, 是否解析成功)。
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, False
    s = str(val).strip()
    if not s or s.lower() in ("null", "nan", ""):
        return None, False
    # 已有 yyyy/mm/dd
    if re.match(r"^\d{4}/\d{1,2}/\d{1,2}$", s):
        parts = s.split("/")
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        if 1 <= m <= 12 and 1 <= d <= 31:
            return f"{y:04d}/{m:02d}/{d:02d}", True
    # 12 21 2022 12:00AM 等
    m = re.match(r"^\s*(\d{1,2})\s+(\d{1,2})\s+(\d{4})", s)
    if m:
        try:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}/{month:02d}/{day:02d}", True
        except (ValueError, IndexError):
            pass
    # 2025-03 或 2025-03-01 或 2016-12-22 12:37:30
    m = re.match(r"^(\d{4})-(\d{1,2})(?:-(\d{1,2}))?", s)
    if m:
        try:
            y, mo = int(m.group(1)), int(m.group(2))
            d = int(m.group(3)) if m.group(3) else 1
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}/{mo:02d}/{d:02d}", True
        except (ValueError, IndexError):
            pass
    # pd.to_datetime 兜底
    try:
        dt = pd.to_datetime(s)
        if pd.notna(dt):
            return dt.strftime("%Y/%m/%d"), True
    except Exception:
        pass
    return None, False


def _apply_date_cleaning(
    df: pd.DataFrame, report: Dict[str, Any]
) -> pd.DataFrame:
    """对所有日期相关列统一为 yyyy/mm/dd，并写日期清洗结果。"""
    date_cols = [
        c for c in df.columns
        if any(kw in c for kw in DATE_LIKE_KEYWORDS)
    ]
    if not date_cols:
        report["date_clean_success"] = 0
        report["date_clean_fail"] = 0
        report["date_unknown_formats"] = []
        return df
    df = df.copy()
    success_per_cell = []
    unknown_formats_set = set()
    for col in date_cols:
        result_col = f"{col}_日期清洗结果" if col + "_日期清洗结果" not in df.columns else col + "_日期清洗结果"
        cleaned = []
        flags = []
        for v in df[col]:
            out, ok = _parse_date_to_ymd(v)
            cleaned.append(out if out else str(v))
            flags.append(1 if ok else 0)
            if not ok and str(v).strip() and str(v).lower() not in ("null", "nan", ""):
                unknown_formats_set.add(str(v)[:80])
        df[col] = cleaned
        if result_col not in df.columns:
            df[result_col] = flags
        success_per_cell.extend(flags)
    report["date_clean_success"] = sum(success_per_cell)
    report["date_clean_fail"] = len(success_per_cell) - report["date_clean_success"]
    report["date_unknown_formats"] = sorted(unknown_formats_set)[:50]
    return df


def _power_to_kw(val: Any) -> Tuple[Optional[float], bool]:
    """
    将功率值换算为 kW。返回 (数值, 是否发生了 W->kW 换算)。
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, False
    s = str(val).strip().upper()
    if not s:
        return None, False
    # 提取数字部分
    num_match = re.search(r"[\d.]+", s)
    if not num_match:
        return None, False
    try:
        num = float(num_match.group())
    except ValueError:
        return None, False
    converted = False
    if "KW" in s or "K W" in s or "千瓦" in s:
        return round(num, 6), False
    if "W" in s and "KW" not in s:
        num = num / 1000.0
        converted = True
    elif re.match(r"^\s*[\d.]+\s*$", str(val).strip()) and num > 1000:
        num = num / 1000.0
        converted = True
    return round(num, 6), converted


def _voltage_to_v(val: Any) -> Optional[float]:
    """电压换算为 V。kV * 1000。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().upper()
    if not s:
        return None
    num_match = re.search(r"[\d.]+", s)
    if not num_match:
        return None
    try:
        num = float(num_match.group())
    except ValueError:
        return None
    if "KV" in s or "K V" in s or "千伏" in s:
        return round(num * 1000, 2)
    return round(num, 2)


def _current_to_a(val: Any) -> Optional[float]:
    """电流：剔除 A 后保留数字。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().upper().replace("A", "").strip()
    if not s:
        return None
    num_match = re.search(r"[\d.]+", s)
    if not num_match:
        return None
    try:
        return round(float(num_match.group()), 2)
    except ValueError:
        return None


def _apply_numeric_cleaning(
    df: pd.DataFrame,
    report: Dict[str, Any],
    do_power: bool = True,
    do_voltage: bool = True,
    do_current: bool = True,
) -> pd.DataFrame:
    """功率 -> kW，电压 -> V，电流 -> A。可按需只执行其中一部分。"""
    df = df.copy()
    if do_power:
        w_to_kw_count = 0
        for col in POWER_COLUMNS:
            if col not in df.columns:
                continue
            new_vals = []
            for v in df[col]:
                num, converted = _power_to_kw(v)
                if converted:
                    w_to_kw_count += 1
                new_vals.append(num)
            df[col] = new_vals
        report["power_w_to_kw_count"] = w_to_kw_count
    if do_voltage:
        for col in VOLTAGE_COLUMNS:
            if col not in df.columns:
                continue
            df[col] = [_voltage_to_v(v) for v in df[col]]
    if do_current:
        for col in CURRENT_COLUMNS:
            if col not in df.columns:
                continue
            df[col] = [_current_to_a(v) for v in df[col]]
    return df


def _apply_station_specific(
    df: pd.DataFrame, report: Dict[str, Any]
) -> pd.DataFrame:
    """充电站专项：充电站内部编号缺失记录。"""
    if STATION_INNER_ID_COL not in df.columns:
        report["station_inner_id_missing_rows"] = []
        return df
    missing = []
    for i, row in df.iterrows():
        v = row.get(STATION_INNER_ID_COL)
        if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
            missing.append({"行号": i + 1, "序号": row.get("序号", i + 1)})
    report["station_inner_id_missing_rows"] = missing
    return df


def _apply_pile_specific(
    df: pd.DataFrame,
    report: Dict[str, Any],
    do_device_type: bool = True,
    do_open_time: bool = True,
) -> pd.DataFrame:
    """充电桩专项：设备类型标准化 A/B/C，设备开通时间校验。可按需只执行其中一部分。"""
    df = df.copy()
    anomaly_rows = []
    if do_device_type and DEVICE_TYPE_COL in df.columns:
        def map_device_type(val):
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return val
            s = str(val).strip()
            if not s:
                return s
            has_chinese = bool(re.search(r"[\u4e00-\u9fff]", s))
            if not has_chinese and "1" in s:
                return "交流"
            if not has_chinese and "2" in s:
                return "直流"
            return val
        df[DEVICE_TYPE_COL] = df[DEVICE_TYPE_COL].apply(map_device_type)
    if do_open_time and OPEN_TIME_COL in df.columns:
        now = datetime.now()
        for i, row in df.iterrows():
            v = row.get(OPEN_TIME_COL)
            out, ok = _parse_date_to_ymd(v)
            if ok and out:
                try:
                    dt = datetime.strptime(out, "%Y/%m/%d")
                    if dt > now:
                        anomaly_rows.append({"行号": i + 1, "序号": row.get("序号", i + 1), "设备开通时间": str(v)})
                except ValueError:
                    pass
    report["pile_open_time_anomaly_rows"] = anomaly_rows
    return df


def clean_dataframe(
    df: pd.DataFrame,
    table_type: Optional[str] = None,
    rules_to_apply: Optional[Iterable[str]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    对充电站或充电桩表执行清洗（规范 V2.0）。
    table_type: "station" | "pile" | None（None 时按列名自动识别）。
    rules_to_apply: 若为 None 则应用该类型全部规则；否则仅应用集合中的规则 ID。
    返回 (清洗后 DataFrame, 报告字典)，报告含 applied_rules: [(rule_id, 中文名), ...]。
    """
    empty_report = {
        "date_clean_success": 0,
        "date_clean_fail": 0,
        "date_unknown_formats": [],
        "power_w_to_kw_count": 0,
        "station_inner_id_missing_rows": [],
        "pile_open_time_anomaly_rows": [],
        "applied_rules": [],
    }
    if df is None or df.empty:
        return df, empty_report
    report = {k: v for k, v in empty_report.items()}
    t = table_type or _detect_table_type(df)
    rule_order = RULES_STATION if t == "station" else RULES_PILE
    to_apply: Set[str] = set(rules_to_apply) if rules_to_apply is not None else set(rule_order)
    applied: List[Tuple[str, str]] = []

    for rule_id in rule_order:
        if rule_id not in to_apply:
            continue
        label = RULE_LABELS.get(rule_id, rule_id)
        applied.append((rule_id, label))
        if rule_id == RULE_NULL_STD:
            df = _standardize_nulls(df)
        elif rule_id == RULE_SEQUENCE:
            df = _ensure_sequence_column(df)
        elif rule_id == RULE_LOCATION:
            df = _truncate_location(df)
        elif rule_id == RULE_DATE:
            df = _apply_date_cleaning(df, report)
        elif rule_id == RULE_POWER:
            df = _apply_numeric_cleaning(
                df, report, do_power=True, do_voltage=False, do_current=False
            )
        elif rule_id == RULE_VOLTAGE:
            df = _apply_numeric_cleaning(
                df, report, do_power=False, do_voltage=True, do_current=False
            )
        elif rule_id == RULE_CURRENT:
            df = _apply_numeric_cleaning(
                df, report, do_power=False, do_voltage=False, do_current=True
            )
        elif rule_id == RULE_STATION_INNER_ID and t == "station":
            df = _apply_station_specific(df, report)
        elif rule_id == RULE_PILE_DEVICE_TYPE and t == "pile":
            df = _apply_pile_specific(df, report, do_device_type=True, do_open_time=False)
        elif rule_id == RULE_PILE_OPEN_TIME and t == "pile":
            df = _apply_pile_specific(df, report, do_device_type=False, do_open_time=True)
    report["applied_rules"] = applied
    return df, report
