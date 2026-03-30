# handlers/data_clean_handler.py - 充电数据清洗与标准化（规范 V2.0）

import re
import hashlib
import pandas as pd
from typing import Optional, Tuple, Dict, List, Any, Set, Iterable
import math
import numpy as np
from datetime import datetime

# 规则 ID（用于自定义清洗勾选与 applied_rules 报告）
RULE_NULL_STD = "null_std"
RULE_UID = "uid"
RULE_SEQUENCE = "sequence"
RULE_LOCATION = "location"
RULE_DATE = "date"
RULE_POWER = "power"
RULE_VOLTAGE = "voltage"
RULE_CURRENT = "current"
RULE_STATION_INNER_ID = "station_inner_id"
RULE_PILE_DEVICE_TYPE = "pile_device_type"
RULE_PILE_OPEN_TIME = "pile_open_time"
RULE_PSQL_COL_ORDER = "psql_col_order"
RULE_LATLON = "latlon_std"
RULE_PHONE = "phone_std"
RULE_DATE_ZERO_DAY = "date_zero_day"
RULE_METER_MAX_LEN = "meter_max_len"

RULE_LABELS = {
    RULE_NULL_STD: "空值标准化",
    RULE_UID: "主键（uid，复合字段哈希）",
    RULE_SEQUENCE: "序号列（递增，可设起始）",
    RULE_LOCATION: "充电站位置截断（≤600字）",
    RULE_DATE: "日期清洗（yyyy-mm-dd + 结果列）",
    RULE_POWER: "功率→kW",
    RULE_VOLTAGE: "电压→V",
    RULE_CURRENT: "电流→A",
    RULE_STATION_INNER_ID: "充电站内部编号缺失校验",
    RULE_PILE_DEVICE_TYPE: "设备类型标准化（交流/直流）",
    RULE_PILE_OPEN_TIME: "设备开通时间校验",
    RULE_PSQL_COL_ORDER: "根据psql调整数据顺序",
    RULE_LATLON: "经纬度字段不合理清洗",
    RULE_PHONE: "联系电话过长审查",
    RULE_DATE_ZERO_DAY: "日期中00日处理",
    RULE_METER_MAX_LEN: "电表号过长处理",
}

# 根据 psql 调整数据顺序：必补四列
PSQL_FOUR_COLS = ["上报机构", "运营商类型", "充电桩生产厂商名称", "充电桩生产厂商类型"]
# 参考列顺序（54 列，与《数据清洗规则》7.4 一致）
REFERENCE_COLUMNS_PSQL = [
    "uid", "序号", "上报机构", "充电桩编号", "充电桩内部编号", "省份", "城市", "区县",
    "经度", "纬度", "经纬度标准", "充电桩类型", "充电桩所属区域分类", "所属充电站编号",
    "充电站内部编号", "充电站名称", "充电站位置", "充电站投入使用时间", "充电站所处道路属性",
    "充电站联系电话", "充电桩所属运营商", "电表号", "充电桩厂商编号", "充电桩型号", "充电桩属性",
    "充电桩生产日期", "服务时间", "桩型号是否获得联盟标识授权", "支付方式", "设备开通时间",
    "额定电压上限", "额定电压下限", "额定电流上限", "额定电流下限", "额定功率",
    "接口数量", "接口1标准", "接口2标准", "接口3标准", "接口4标准", "备注",
    "省份_中文", "城市_中文", "区县_中文", "充电桩类型_转换", "充电桩属性_转换",
    "充电桩所属运营商_转换", "充电桩厂商编号_转换", "入库时间", "运营商名称", "运营商类型",
    "充电桩内部编号_运营商名称", "充电桩生产厂商名称", "充电桩生产厂商类型",
]

# 主键 uid：复合字段（按顺序），见《数据清洗规则》1.2
UID_COLUMN = "uid"
UID_KEY_STATION = ["充电站内部编号", "充电站名称"]
UID_KEY_PILE = ["充电桩编号", "所属充电站编号", "充电站内部编号", "序号"]

# 按表类型适用的规则（执行顺序）
RULES_STATION = [
    RULE_NULL_STD, RULE_UID, RULE_SEQUENCE, RULE_LOCATION, RULE_DATE,
    RULE_DATE_ZERO_DAY, RULE_METER_MAX_LEN,
    RULE_POWER, RULE_VOLTAGE, RULE_CURRENT, RULE_STATION_INNER_ID,
    RULE_LATLON, RULE_PHONE,
    RULE_PSQL_COL_ORDER,
]
# 充电桩表：序号在 uid 之前（自定义清洗可取消勾选序号）；若表内已有有效序号则 uid 直接使用
RULES_PILE = [
    RULE_NULL_STD, RULE_SEQUENCE, RULE_UID, RULE_LOCATION, RULE_DATE,
    RULE_DATE_ZERO_DAY, RULE_METER_MAX_LEN,
    RULE_POWER, RULE_VOLTAGE, RULE_CURRENT,
    RULE_PILE_DEVICE_TYPE, RULE_PILE_OPEN_TIME,
    RULE_LATLON, RULE_PHONE,
    RULE_PSQL_COL_ORDER,
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
# 日期清洗：适用列 = 以下固定字段 或 列名包含「时间」「日期」；排除列名为「服务时间」
DATE_COLUMN_EXACT = ("充电站投入使用时间", "设备开通时间", "入库时间", "充电桩生产日期")
DATE_LIKE_KEYWORDS = ("时间", "日期")
DATE_EXCLUDE_COLUMN = "服务时间"
# PostgreSQL 日期时间入库：Excel 序列号合理区间（与 WPS/Excel 1900 系统一致，由 openpyxl 换算）；排除 0、1 以免与 0/1 标记混淆
DATE_RESULT_COL_SUFFIX = "_日期清洗结果"
DATE_COLUMN_EXCLUDE_FLAG_SUFFIXES = ("_标记", "_标志")  # 列名以这些结尾视为 0/1 类标记列，不参与 Excel 序列号转换
EXCEL_SERIAL_MAX = 100_000.0
EXCEL_SERIAL_MIN_EXCLUSIVE = 1.0  # 仅接受 serial > 1（排除 0、1）
LOCATION_MAX_LEN = 600
STATION_INNER_ID_COL = "充电站内部编号"
LOCATION_COL = "充电站位置"
OPEN_TIME_COL = "设备开通时间"
DEVICE_TYPE_COL = "充电桩类型"
# 经纬度：物理范围与小数位（整数部分由范围自然满足 ≤3 位）
LAT_COL = "纬度"
LON_COL = "经度"
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0
COORD_DECIMALS = 6
# 充电站联系电话：仅保留数字与英文逗号，总长≤50
PHONE_COL = "充电站联系电话"
PHONE_MAX_LEN = 50
PHONE_EMPTY_KEYWORDS = ("无", "n/a", "na", "null", "无无", "-", "—")
# 电表号列名包含「电表号」时截断长度
METER_MAX_LEN = 50
METER_NAME_KEYWORD = "电表号"


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


def _sequence_column_has_values(df: pd.DataFrame) -> bool:
    """序号列存在且至少有一行非空（充电桩 uid 复合用）。"""
    if "序号" not in df.columns:
        return False
    for v in df["序号"]:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s and s.lower() not in ("null", "nan", ""):
            return True
    return False


def _ensure_uid_column(
    df: pd.DataFrame, table_type: str, sequence_start: int = 1
) -> pd.DataFrame:
    """
    按《数据清洗规则》1.2 生成主键列 uid：复合字段按顺序用 | 拼接后 MD5 十六进制。
    充电站：充电站内部编号、充电站名称；充电桩：充电桩编号、所属充电站编号、充电站内部编号、序号。
    充电桩：若未勾选「序号」规则但表内无数号或序号全空，则按 sequence_start 自动生成序号列后再算 uid。
    若某行参与复合的字段全为空，则用行号哈希避免重复。
    """
    df = df.copy()
    if table_type == "pile" and not _sequence_column_has_values(df):
        df = _ensure_sequence_column(df, start=sequence_start)
    key_cols = UID_KEY_STATION if table_type == "station" else UID_KEY_PILE
    # 只使用表中存在的列，缺失列视为空字符串参与拼接
    existing = [c for c in key_cols if c in df.columns]
    uids = []
    for i in range(len(df)):
        parts = []
        for c in existing:
            v = df.iloc[i][c]
            if v is None or (isinstance(v, float) and pd.isna(v)):
                parts.append("")
            else:
                parts.append(str(v).strip())
        raw = "|".join(parts)
        if not raw or all(p == "" for p in parts):
            raw = f"__row_{i}"
        uids.append(hashlib.md5(raw.encode("utf-8")).hexdigest())
    if UID_COLUMN in df.columns:
        df = df.drop(columns=[UID_COLUMN])
    df.insert(0, UID_COLUMN, uids)
    return df


def _ensure_sequence_column(df: pd.DataFrame, start: int = 1) -> pd.DataFrame:
    """生成递增序号列（列名：序号），若已有则覆盖。start 为起始值（含）。"""
    df = df.copy()
    if start < 1:
        start = 1
    if "序号" in df.columns:
        df = df.drop(columns=["序号"])
    n = len(df)
    df.insert(0, "序号", range(start, start + n))
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
    尝试将单个值解析为 yyyy-mm-dd（YMD，不含时分秒）。缺失日补 1。
    返回 (标准化日期字符串 或 None, 是否解析成功)。
    支持：中文(2023年8月29日)、斜杠(2023/3/11)、紧凑(20200920)、标准横杠、美式(10 13 2022 12:00AM)等。
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, False
    s = str(val).strip()
    if not s or s.lower() in ("null", "nan", ""):
        return None, False
    # 中文：2023年8月29日、2023年08月29日
    m = re.match(r"^(\d{4})年(\d{1,2})月(\d{1,2})日?$", s)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}", True
        except (ValueError, IndexError):
            pass
    # 斜杠：2023/3/11、2023/03/11、2023/3、2023/8
    if re.match(r"^\d{4}/\d{1,2}(?:/\d{1,2})?(?:\s|$)", s) or re.match(r"^\d{4}/\d{1,2}(?:/\d{1,2})?$", s):
        parts = re.split(r"[/\s]+", s)[:3]
        if len(parts) >= 2:
            try:
                y, mo = int(parts[0]), int(parts[1])
                d = int(parts[2]) if len(parts) > 2 else 1
                if 1 <= mo <= 12 and 1 <= d <= 31:
                    return f"{y:04d}-{mo:02d}-{d:02d}", True
            except (ValueError, IndexError):
                pass
    # 紧凑数字：20200920
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}", True
        except (ValueError, IndexError):
            pass
    # 标准横杠：2021-03-03、2022-11-20 06:02:03、2025-03
    m = re.match(r"^(\d{4})-(\d{1,2})(?:-(\d{1,2}))?(?:\s|$)", s)
    if m:
        try:
            y, mo = int(m.group(1)), int(m.group(2))
            d = int(m.group(3)) if m.group(3) else 1
            if 1 <= mo <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{mo:02d}-{d:02d}", True
        except (ValueError, IndexError):
            pass
    # 美式：10 13 2022 12:00AM、12 21 2022 12:00AM
    m = re.match(r"^\s*(\d{1,2})\s+(\d{1,2})\s+(\d{4})", s)
    if m:
        try:
            month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}", True
        except (ValueError, IndexError):
            pass
    # 纯数字串（含小数）：上层已处理 Excel 序列号与 8 位 yyyymmdd；此处不再让 pandas 误解析（如 45138）
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        return None, False
    # pd.to_datetime 兜底（源数据 20220100 等会解析成 day=0，统一将日改为 01）
    try:
        dt = pd.to_datetime(s)
        if pd.notna(dt):
            d = getattr(dt, "day", None)
            mo = getattr(dt, "month", None)
            if d is not None and mo is not None and 1 <= mo <= 12:
                if d == 0:
                    d = 1
                if 1 <= d <= 31:
                    return f"{dt.year:04d}-{mo:02d}-{d:02d}", True
    except Exception:
        pass
    return None, False


def _column_allows_excel_serial(col_str: str) -> bool:
    """结果列、明显标记列不做 Excel 序列号转换。"""
    if col_str.endswith(DATE_RESULT_COL_SUFFIX):
        return False
    if any(col_str.endswith(sfx) for sfx in DATE_COLUMN_EXCLUDE_FLAG_SUFFIXES):
        return False
    return True


def _try_keep_valid_datetime_text(s: str) -> Optional[str]:
    """若已是 PostgreSQL 可接受的 ISO 风格日期/时间文本，原样保留（仅 strip）。"""
    raw = s.strip()
    if not raw:
        return None
    candidates = [raw]
    if " " in raw and "T" not in raw[:13]:
        candidates.append(raw.replace(" ", "T", 1))
    for c in candidates:
        try:
            datetime.fromisoformat(c)
            return raw
        except ValueError:
            continue
    return None


def _days_in_month_scalar(y: int, mo: int) -> int:
    if mo == 2:
        leap = (y % 4 == 0) and ((y % 100 != 0) or (y % 400 == 0))
        return 29 if leap else 28
    if mo in (4, 6, 9, 11):
        return 30
    return 31


def _days_in_month_vectorized(y: np.ndarray, mo: np.ndarray) -> np.ndarray:
    """与 y, mo 同形状；mo 应在 1..12。"""
    dim = np.full(y.shape, 31, dtype=np.int16)
    m30 = (mo == 4) | (mo == 6) | (mo == 9) | (mo == 11)
    dim = np.where(m30, 30, dim)
    feb = mo == 2
    leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0))
    dim = np.where(feb & leap, 29, dim)
    dim = np.where(feb & ~leap, 28, dim)
    return dim


def _vector_fix_compact_yyyymmdd_column(ser: pd.Series) -> Tuple[pd.Series, int, int]:
    """
    8 位 yyyyMMdd（纯数字字符串，或 [1e7,1e8) 内整数/浮点）：日向不合法（含 00、大于当月天数）一律改为 01；
    月不在 1–12 则置空。向量化，按列一次扫描。
    返回 (新列, 日改为_01_的单元格数, 非法月置空单元格数)。
    """
    out = ser.copy()
    if len(ser) == 0:
        return out, 0, 0
    s = ser.astype(str).str.strip()
    s = s.mask(s.str.lower().isin(["nan", "none"]), "")
    mask8s = s.str.fullmatch(r"\d{8}", na=False)
    num = pd.to_numeric(ser, errors="coerce")
    imask = (
        num.notna()
        & (num == np.floor(num))
        & (num >= 10_000_000)
        & (num < 100_000_000)
    )
    proc = mask8s | imask
    if not proc.any():
        return out, 0, 0
    s8 = pd.Series("", index=ser.index, dtype=object)
    s8[imask] = num[imask].astype(np.int64).astype(str).str.zfill(8)
    s8[mask8s] = s[mask8s]
    sub = s8[proc]
    y = sub.str[:4].to_numpy(dtype=np.int32)
    mo = sub.str[4:6].to_numpy(dtype=np.int32)
    dd = sub.str[6:8].to_numpy(dtype=np.int32)
    valid_m = (mo >= 1) & (mo <= 12)
    maxd = np.zeros(len(y), dtype=np.int16)
    if valid_m.any():
        vm = valid_m
        maxd[vm] = _days_in_month_vectorized(y[vm], mo[vm])
    dd_new = dd.copy()
    bad_day = valid_m & ((dd == 0) | (dd > maxd))
    dd_new[bad_day] = 1
    invalid_m = ~valid_m
    y_ser = pd.Series(y, index=sub.index).astype(str).str.zfill(4)
    mo_ser = pd.Series(mo, index=sub.index).astype(str).str.zfill(2)
    dn_ser = pd.Series(dd_new, index=sub.index).astype(str).str.zfill(2)
    parts = (y_ser + "-" + mo_ser + "-" + dn_ser).to_numpy(dtype=object)
    parts[invalid_m] = ""
    out.loc[sub.index] = parts
    return out, int(bad_day.sum()), int(invalid_m.sum())


def _try_compact_yyyymmdd_str(s: str) -> Optional[str]:
    """None=非 8 位数字日期形态；''=月非法须置空；否则 YYYY-MM-DD（日非法已改为 01）。"""
    t = s.strip()
    if not re.match(r"^\d{8}$", t):
        return None
    try:
        y, mo, d = int(t[:4]), int(t[4:6]), int(t[6:8])
    except ValueError:
        return None
    if not (1 <= mo <= 12):
        return ""
    mx = _days_in_month_scalar(y, mo)
    if d == 0 or d > mx:
        d = 1
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _excel_serial_candidate_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and (math.isnan(val) or pd.isna(val)):
            return None
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    if isinstance(val, str):
        t = val.strip()
        if not t:
            return None
        if not re.fullmatch(r"-?\d+(\.\d+)?", t):
            return None
        try:
            f = float(t)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except ValueError:
            return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _try_excel_serial_to_pg_text(val: Any) -> Optional[str]:
    """
    Excel 1900 日期系统：与 Excel/WPS 一致使用 openpyxl.utils.datetime.from_excel。
    输出：无小数部分为 YYYY-MM-DD；含时间为 YYYY-MM-DD HH:MM:SS。
    """
    sn = _excel_serial_candidate_float(val)
    if sn is None:
        return None
    if not (EXCEL_SERIAL_MIN_EXCLUSIVE < sn <= EXCEL_SERIAL_MAX):
        return None
    try:
        from openpyxl.utils.datetime import from_excel

        dt = from_excel(sn)
    except Exception:
        return None
    if dt.year < 1900 or dt.year > 2200:
        return None
    if abs(sn - round(sn)) < 1e-9:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _format_dt_for_pg(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt.strftime("%Y-%m-%d")
    if dt.microsecond:
        frac = f"{dt.microsecond:06d}".rstrip("0")
        if frac:
            return dt.strftime("%Y-%m-%d %H:%M:%S") + "." + frac
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_date_cell_for_pg(val: Any, col_name: str) -> Tuple[str, int, bool]:
    """
    日期时间列单格清洗：目标为 PostgreSQL timestamp/date 可解析文本。
    返回 (新值, 结果标记 1=原空或成功/0=原非空且失败, 是否由 Excel 序列号得到)。
    无法解析的非空值：置空字符串。
    """
    col_str = str(col_name).strip()
    allow_excel = _column_allows_excel_serial(col_str)

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "", 1, False
    if isinstance(val, bool):
        return "", 0, False
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return "", 1, False
        return _format_dt_for_pg(val.to_pydatetime()), 1, False
    if isinstance(val, datetime):
        return _format_dt_for_pg(val), 1, False

    s0 = str(val).strip()
    if not s0 or s0.lower() in ("null", "nan", ""):
        return "", 1, False

    kept = _try_keep_valid_datetime_text(s0)
    if kept is not None:
        return kept, 1, False

    fast = _try_fast_ymd_parse(s0)
    if fast is not None:
        out, _ok = fast
        return out, 1, False

    c8 = _try_compact_yyyymmdd_str(s0)
    if c8 is not None:
        if c8 == "":
            return "", 0, False
        return c8, 1, False

    if allow_excel:
        ex = _try_excel_serial_to_pg_text(val)
        if ex is not None:
            return ex, 1, True

    out, ok = _parse_date_to_ymd(s0)
    if ok and out:
        return out, 1, False
    return "", 0, False


def _parsed_datetime_from_cleaned_cell(val: Any) -> Optional[datetime]:
    """设备开通时间等校验：从清洗后的单元格解析 datetime（支持日期或带时间）。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.to_pydatetime()
    if isinstance(val, datetime):
        return val
    sn = _excel_serial_candidate_float(val)
    if sn is not None and EXCEL_SERIAL_MIN_EXCLUSIVE < sn <= EXCEL_SERIAL_MAX:
        try:
            from openpyxl.utils.datetime import from_excel

            dt = from_excel(sn)
            if 1900 <= dt.year <= 2200:
                return dt.replace(tzinfo=None)
        except Exception:
            pass
    s = str(val).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError:
        pass
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        return None
    try:
        ts = pd.to_datetime(s)
        if pd.notna(ts):
            return ts.to_pydatetime()
    except Exception:
        pass
    return None


def _get_date_columns(df: pd.DataFrame) -> List[str]:
    """适用日期清洗的列：固定字段 或 列名包含「时间」「日期」；排除「服务时间」、结果列、标记列。"""
    cols = []
    for c in df.columns:
        c_str = str(c).strip()
        if c_str == DATE_EXCLUDE_COLUMN:
            continue
        if c_str.endswith(DATE_RESULT_COL_SUFFIX):
            continue
        if any(c_str.endswith(sfx) for sfx in DATE_COLUMN_EXCLUDE_FLAG_SUFFIXES):
            continue
        if c_str in DATE_COLUMN_EXACT:
            cols.append(c)
            continue
        if any(kw in c_str for kw in DATE_LIKE_KEYWORDS):
            cols.append(c)
    return cols


def _try_fast_ymd_parse(val: Any) -> Optional[Tuple[str, bool]]:
    """已是 yyyy-mm-dd 且日月合法时快速返回，避免走完整解析链。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return None
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return None
    try:
        y, mo, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}", True
    except ValueError:
        pass
    return None


def _apply_date_cleaning(
    df: pd.DataFrame, report: Dict[str, Any]
) -> pd.DataFrame:
    """
    对适用列清洗为 PostgreSQL date/timestamp 可解析文本（YYYY-MM-DD 或带时分秒）；
    支持 Excel 序列号、合法 ISO 文本原样保留；无法解析的非空值置空。
    写 xxx_日期清洗结果（1=原空或成功，0=原非空且失败）。
    8 位 yyyyMMdd 在向量化预处理后进入单格逻辑；横杠形式日=00 由「日期中00日处理」处理。
    """
    date_cols = _get_date_columns(df)
    if not date_cols:
        report["date_clean_success"] = 0
        report["date_clean_fail"] = 0
        report["date_unknown_formats"] = []
        report["date_excel_serial_converted_count"] = 0
        report["date_yyyymmdd_day_to_01_count"] = 0
        report["date_yyyymmdd_invalid_month_cleared_count"] = 0
        return df
    df = df.copy()
    success_per_cell = []
    unknown_formats_set = set()
    excel_serial_total = 0
    ymd01_total = 0
    ymd_bad_m_total = 0
    for col in date_cols:
        df[col], n01, nbm = _vector_fix_compact_yyyymmdd_column(df[col])
        ymd01_total += n01
        ymd_bad_m_total += nbm
        result_col = f"{col}_日期清洗结果"
        cleaned = []
        flags = []
        for v in df[col]:
            _empty = v is None or (isinstance(v, float) and pd.isna(v)) or not str(v).strip() or str(v).strip().lower() in ("null", "nan")
            new_s, cell_flag, was_excel = _normalize_date_cell_for_pg(v, col)
            if was_excel:
                excel_serial_total += 1
            cleaned.append(new_s)
            flags.append(cell_flag)
            if cell_flag == 0 and not _empty:
                unknown_formats_set.add(str(v)[:80])
        df[col] = cleaned
        df[result_col] = flags
        success_per_cell.extend(flags)
    report["date_clean_success"] = sum(success_per_cell)
    report["date_clean_fail"] = len(success_per_cell) - report["date_clean_success"]
    report["date_unknown_formats"] = sorted(unknown_formats_set)[:50]
    report["date_excel_serial_converted_count"] = excel_serial_total
    report["date_yyyymmdd_day_to_01_count"] = ymd01_total
    report["date_yyyymmdd_invalid_month_cleared_count"] = ymd_bad_m_total
    return df


def _apply_date_zero_day_fix(df: pd.DataFrame, report: Dict[str, Any]) -> pd.DataFrame:
    """
    日期适用列中，单元格为严格 yyyy-mm-00 的改为 yyyy-mm-01（向量化）。
    """
    df = df.copy()
    date_cols = _get_date_columns(df)
    total = 0
    if not date_cols:
        report["date_zero_day_fixed_count"] = 0
        return df
    for col in date_cols:
        s = df[col].astype(str)
        mask = s.str.match(r"^\d{4}-\d{2}-00$", na=False)
        n = int(mask.sum())
        if n:
            df.loc[mask, col] = s[mask].str.slice(0, 8) + "01"
            total += n
    report["date_zero_day_fixed_count"] = total
    return df


def _apply_meter_max_len(df: pd.DataFrame, report: Dict[str, Any]) -> pd.DataFrame:
    """
    列名包含「电表号」的列，字符串截断至 METER_MAX_LEN（向量化）。
    """
    df = df.copy()
    total_trunc = 0
    if not any(METER_NAME_KEYWORD in str(c) for c in df.columns):
        report["meter_truncated_count"] = 0
        return df
    for c in df.columns:
        if METER_NAME_KEYWORD not in str(c):
            continue
        ser = df[c]
        s = ser.astype(str)
        long_mask = s.str.len() > METER_MAX_LEN
        total_trunc += int(long_mask.sum())
        df[c] = s.str.slice(0, METER_MAX_LEN)
    report["meter_truncated_count"] = total_trunc
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
            dt = _parsed_datetime_from_cleaned_cell(v)
            if dt is not None:
                try:
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                    if dt > now:
                        anomaly_rows.append({"行号": i + 1, "序号": row.get("序号", i + 1), "设备开通时间": str(v)})
                except (TypeError, ValueError):
                    pass
    report["pile_open_time_anomaly_rows"] = anomaly_rows
    return df


def _apply_latlon_cleaning(df: pd.DataFrame, report: Dict[str, Any]) -> pd.DataFrame:
    """
    经纬度字段不合理清洗：纬度[-90,90]、经度[-180,180]，保留6位小数，异常值标记为空。
    报告：latlon_invalid_rows（行号、列、原值、修正方式）、latlon_invalid_count。
    使用向量化操作避免逐行 iloc，提升大表性能。
    """
    df = df.copy()
    invalid_rows: List[Dict[str, Any]] = []
    total_invalid = 0
    for col, vmin, vmax in [(LAT_COL, LAT_MIN, LAT_MAX), (LON_COL, LON_MIN, LON_MAX)]:
        if col not in df.columns:
            continue
        ser = df[col]
        numeric = pd.to_numeric(ser, errors="coerce")
        rounded = numeric.round(COORD_DECIMALS)
        int_part = rounded.fillna(0).astype(int)
        in_range = (numeric >= vmin) & (numeric <= vmax)
        valid = in_range & (int_part.abs() < 1000)
        df[col] = rounded.where(valid)
        invalid_mask = ~valid
        total_invalid += int(invalid_mask.sum())
        non_numeric = pd.isna(numeric) & ser.astype(str).str.strip().ne("")
        out_of_range = pd.notna(numeric) & ~in_range
        inv_positions = invalid_mask.to_numpy().nonzero()[0][:500]
        for pos in inv_positions:
            o = str(ser.iloc[pos])[:50]
            if non_numeric.iloc[pos]:
                reason = "标记为NULL"
            elif out_of_range.iloc[pos]:
                reason = "超出范围，标记为NULL"
            else:
                reason = "整数位超限，标记为NULL"
            invalid_rows.append({"行号": int(pos) + 1, "列": col, "原值": o, "修正方式": reason})
    report["latlon_invalid_rows"] = invalid_rows
    report["latlon_invalid_count"] = total_invalid
    return df


def _apply_phone_cleaning(df: pd.DataFrame, report: Dict[str, Any]) -> pd.DataFrame:
    """
    充电站联系电话：仅保留0-9和英文逗号；总长≤50截断；无/ N/A等置空。
    报告：phone_cleaned_count、phone_abnormal_examples。向量化实现以提升大表性能。
    """
    df = df.copy()
    if PHONE_COL not in df.columns:
        report["phone_cleaned_count"] = 0
        report["phone_abnormal_examples"] = []
        return df
    ser = df[PHONE_COL]
    orig = ser.astype(str).str.strip().replace("nan", "").replace("None", "")
    empty_mask = (orig == "") | orig.str.lower().isin(PHONE_EMPTY_KEYWORDS)
    filtered = orig.str.replace(r"[^0-9,]", "", regex=True)
    new = filtered.str[: PHONE_MAX_LEN]
    new = new.where(~empty_mask, "")
    new = new.fillna("")
    df[PHONE_COL] = new
    changed = (orig != new)
    cleaned_count = int(changed.sum())
    sample_positions = changed.to_numpy().nonzero()[0][:30]
    abnormal_examples = [str(orig.iloc[i])[:80] for i in sample_positions]
    report["phone_cleaned_count"] = cleaned_count
    report["phone_abnormal_examples"] = abnormal_examples
    return df


def _apply_psql_column_order(df: pd.DataFrame, report: Dict[str, Any]) -> pd.DataFrame:
    """
    根据 psql 参考列顺序：1）补全四列；2）补全参考列并记录新增字段；3）按参考顺序重排列。
    """
    df = df.copy()
    # 步骤 1：必补四列
    for col in PSQL_FOUR_COLS:
        if col not in df.columns:
            df[col] = ""
    # 步骤 2：参考列中除四列外，缺失则新增并记录
    added_columns: List[str] = []
    for col in REFERENCE_COLUMNS_PSQL:
        if col not in df.columns:
            df[col] = ""
            added_columns.append(col)
    report["psql_added_columns"] = added_columns
    # 步骤 3：列顺序 = 参考顺序中存在的列 + 表中多出的列
    ordered = [c for c in REFERENCE_COLUMNS_PSQL if c in df.columns]
    extra = [c for c in df.columns if c not in REFERENCE_COLUMNS_PSQL]
    df = df[ordered + extra]
    return df


def clean_dataframe(
    df: pd.DataFrame,
    table_type: Optional[str] = None,
    rules_to_apply: Optional[Iterable[str]] = None,
    sequence_start: int = 1,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    对充电站或充电桩表执行清洗（规范 V2.0）。
    table_type: "station" | "pile" | None（None 时按列名自动识别）。
    rules_to_apply: 若为 None 则应用该类型全部规则；否则仅应用集合中的规则 ID。
    sequence_start: 序号列起始值（执行「序号」规则或充电桩 uid 自动补序号时使用）。
    返回 (清洗后 DataFrame, 报告字典)，报告含 applied_rules: [(rule_id, 中文名), ...]。
    """
    empty_report = {
        "date_clean_success": 0,
        "date_clean_fail": 0,
        "date_unknown_formats": [],
        "date_excel_serial_converted_count": 0,
        "date_yyyymmdd_day_to_01_count": 0,
        "date_yyyymmdd_invalid_month_cleared_count": 0,
        "power_w_to_kw_count": 0,
        "station_inner_id_missing_rows": [],
        "pile_open_time_anomaly_rows": [],
        "psql_added_columns": [],
        "latlon_invalid_rows": [],
        "latlon_invalid_count": 0,
        "phone_cleaned_count": 0,
        "phone_abnormal_examples": [],
        "date_zero_day_fixed_count": 0,
        "meter_truncated_count": 0,
        "applied_rules": [],
    }
    if df is None or df.empty:
        return df, empty_report
    report = {k: v for k, v in empty_report.items()}
    t = table_type or _detect_table_type(df)
    rule_order = RULES_STATION if t == "station" else RULES_PILE
    to_apply: Set[str] = set(rules_to_apply) if rules_to_apply is not None else set(rule_order)
    applied: List[Tuple[str, str]] = []
    seq_start = sequence_start if sequence_start >= 1 else 1

    for rule_id in rule_order:
        if rule_id not in to_apply:
            continue
        label = RULE_LABELS.get(rule_id, rule_id)
        applied.append((rule_id, label))
        if rule_id == RULE_NULL_STD:
            df = _standardize_nulls(df)
        elif rule_id == RULE_UID:
            df = _ensure_uid_column(df, t, sequence_start=seq_start)
        elif rule_id == RULE_SEQUENCE:
            df = _ensure_sequence_column(df, start=seq_start)
        elif rule_id == RULE_LOCATION:
            df = _truncate_location(df)
        elif rule_id == RULE_DATE:
            df = _apply_date_cleaning(df, report)
        elif rule_id == RULE_DATE_ZERO_DAY:
            df = _apply_date_zero_day_fix(df, report)
        elif rule_id == RULE_METER_MAX_LEN:
            df = _apply_meter_max_len(df, report)
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
        elif rule_id == RULE_LATLON:
            df = _apply_latlon_cleaning(df, report)
        elif rule_id == RULE_PHONE:
            df = _apply_phone_cleaning(df, report)
        elif rule_id == RULE_PSQL_COL_ORDER:
            df = _apply_psql_column_order(df, report)
    report["applied_rules"] = applied
    return df, report
