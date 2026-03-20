# handlers/csv_convert_handler.py - 多 Sheet Excel 合并为单个 CSV（UTF-8）

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from io import BytesIO


def excel_sheets_to_csv(
    file_bytes: bytes,
    filename: str,
    engine: Optional[str] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[Dict[str, Any]]]:
    """
    将多 Sheet Excel 按首行表头纵向合并为一个 DataFrame。
    每个 Sheet 第一行为标题行，假定各 Sheet 表头完全一致。
    返回 (合并后的 DataFrame, 错误信息, 统计信息)。
    成功时 error 为 None，stats 含 sheet_names、row_counts 等；失败时 df 为 None，error 为 str。
    """
    if engine is None:
        engine = "xlrd" if (filename.lower().endswith(".xls") and not filename.lower().endswith(".xlsx")) else "openpyxl"
    try:
        all_sheets = pd.read_excel(
            BytesIO(file_bytes),
            sheet_name=None,
            header=0,
            engine=engine,
        )
    except Exception as e:
        return None, f"读取 Excel 失败：{e}", None

    if not all_sheets:
        return None, "该文件不包含任何 Sheet。", None

    dfs = []
    sheet_names = []
    row_counts = []

    for name, df in all_sheets.items():
        if df is None or df.empty:
            sheet_names.append(name)
            row_counts.append(0)
            continue
        dfs.append(df)
        sheet_names.append(name)
        row_counts.append(len(df))

    if not dfs:
        return (
            pd.DataFrame(),
            None,
            {"sheet_names": sheet_names, "row_counts": row_counts, "total_rows": 0},
        )

    try:
        merged = pd.concat(dfs, axis=0, ignore_index=True)
    except Exception as e:
        return None, f"合并 Sheet 时出错：{e}", None

    stats = {
        "sheet_names": sheet_names,
        "row_counts": row_counts,
        "total_rows": len(merged),
        "sheet_count": len(dfs),
    }
    return merged, None, stats


def _read_one_file_merged(
    path: str,
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[Dict[str, Any]]]:
    """
    按路径读取一个 Excel，合并其所有 Sheet 为一个 DataFrame。
    返回 (merged_df, 错误信息, 统计信息)。失败时 df 为 None。
    """
    if not os.path.isfile(path):
        return None, f"文件不存在：{path}", None
    low = path.lower()
    if not (low.endswith(".xlsx") or low.endswith(".xls")):
        return None, "仅支持 .xlsx 或 .xls 文件。", None
    if low.endswith(".xls") and not low.endswith(".xlsx"):
        try:
            all_sheets = pd.read_excel(path, sheet_name=None, header=0, engine="xlrd")
        except Exception as e:
            return None, f"读取 Excel 失败：{e}", None
    else:
        all_sheets = None
        for try_engine in ("calamine", "openpyxl"):
            try:
                all_sheets = pd.read_excel(
                    path,
                    sheet_name=None,
                    header=0,
                    engine=try_engine,
                )
                break
            except Exception:
                continue
        if all_sheets is None:
            return None, "读取 Excel 失败（calamine 与 openpyxl 均不可用或文件不兼容）。", None

    if not all_sheets:
        return None, "该文件不包含任何 Sheet。", None

    dfs = []
    sheet_names = []
    row_counts = []
    for name, df in all_sheets.items():
        if df is None or df.empty:
            sheet_names.append(name)
            row_counts.append(0)
            continue
        dfs.append(df)
        sheet_names.append(name)
        row_counts.append(len(df))

    if not dfs:
        return (
            None,
            "所有 Sheet 均为空。",
            {"sheet_names": sheet_names, "row_counts": row_counts, "total_rows": 0, "sheet_count": 0},
        )
    try:
        merged = pd.concat(dfs, axis=0, ignore_index=True)
    except Exception as e:
        return None, f"合并 Sheet 时出错：{e}", None
    stats = {
        "sheet_names": sheet_names,
        "row_counts": row_counts,
        "total_rows": len(merged),
        "sheet_count": len(dfs),
    }
    return merged, None, stats


def excel_path_to_csv(
    input_path: str,
) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    按本地路径读取多 Sheet Excel，合并后写入与源文件同目录的「原文件名_合并.csv」（UTF-8）。
    返回 (输出 CSV 的完整路径, 错误信息, 统计信息)。成功时 error 为 None；失败时 output_path 为 None。
    """
    raw = input_path.strip().strip('"').strip("'")
    path = os.path.abspath(raw)
    merged, err, stats = _read_one_file_merged(path)
    if err or merged is None:
        return None, err or "读取失败", stats
    dir_path = os.path.dirname(path)
    base_name = os.path.splitext(os.path.basename(path))[0]
    output_path = os.path.join(dir_path, base_name + "_合并.csv")
    try:
        merged.to_csv(output_path, index=False, encoding="utf-8")
    except Exception as e:
        return None, f"写入 CSV 失败：{e}", None
    return output_path, None, stats


def parse_paths_from_multiline(text: str) -> List[str]:
    """将多行字符串解析为路径列表：按行拆分、strip、去引号、去空行，仅保留 .xlsx/.xls。"""
    paths = []
    for line in text.strip().splitlines():
        raw = line.strip().strip('"').strip("'")
        if not raw:
            continue
        path = os.path.abspath(raw)
        low = path.lower()
        if low.endswith(".xlsx") or (low.endswith(".xls") and not low.endswith(".xlsx")):
            paths.append(path)
    return paths


def excel_paths_to_single_csv(
    paths: List[str],
) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    多个 Excel 路径各自按 Sheet 合并后，再纵向拼成一张表，写入一个 CSV。
    保存到第一个文件所在目录，文件名：多表合并_YYYYMMDD_HHMMSS.csv。
    任一文件失败则整体失败。返回 (输出路径, 错误信息, 统计)。
    """
    if not paths:
        return None, "请至少输入一个文件路径。", None
    dfs = []
    file_rows = []
    for i, path in enumerate(paths):
        merged, err, _ = _read_one_file_merged(path)
        if err or merged is None:
            return None, f"第 {i + 1} 个文件失败：{path}\n{err}", None
        dfs.append(merged)
        file_rows.append(len(merged))
    try:
        combined = pd.concat(dfs, axis=0, ignore_index=True)
    except Exception as e:
        return None, f"合并多表时出错：{e}", None
    dir_path = os.path.dirname(paths[0])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(dir_path, f"多表合并_{timestamp}.csv")
    try:
        combined.to_csv(output_path, index=False, encoding="utf-8")
    except Exception as e:
        return None, f"写入 CSV 失败：{e}", None
    stats = {
        "total_rows": len(combined),
        "file_count": len(paths),
        "rows_per_file": file_rows,
    }
    return output_path, None, stats


def excel_paths_to_separate_csvs(
    paths: List[str],
) -> List[Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]]:
    """
    对每个路径调用 excel_path_to_csv，分别写入各自同目录的「原文件名_合并.csv」。
    返回列表，每项为 (该表输出 CSV 路径, 错误信息, 统计信息)。
    """
    results = []
    for path in paths:
        raw = path.strip().strip('"').strip("'")
        out_path, err, stats = excel_path_to_csv(raw)
        results.append((out_path, err, stats))
    return results
