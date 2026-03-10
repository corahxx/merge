#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
诊断「未找到可用的工作表」：对指定 Excel 的每个 Sheet 跑与 _sheet_has_content 相同的逻辑，
打印前 5 行非空单元格数、是否判为有内容、以及 _find_target_sheet 的结论。
用法: python scripts/diagnose_sheet_content.py [文件或目录路径]
      不传参时自动在项目下找含 "12" 的目录并诊断其内所有 xlsx。
      加 --out 时把结果写入 diagnose_sheet_result.txt（UTF-8），便于在合并报错的电脑上对比。
"""

import sys
import os
from pathlib import Path
from io import BytesIO

# 允许从项目根运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd


def sheet_has_content_diagnose(file_bytes: bytes, sheet_name: str, engine: str = "openpyxl", min_nonempty: int = 3):
    """与 table_merge_handler._sheet_has_content 相同逻辑，但返回 (非空数, 是否通过, 异常信息)。"""
    try:
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name, header=None, nrows=5, engine=engine)
        count = int(df.notna().sum().sum())
        return count, count >= min_nonempty, None
    except Exception as e:
        return None, False, str(e)


def find_target_sheet_diagnose(file_bytes: bytes, engine: str = "openpyxl"):
    """与 table_merge_handler._find_target_sheet 相同逻辑，返回 (选中的 sheet, 是否多 sheet 模式, 各 sheet 诊断)。"""
    xl = pd.ExcelFile(BytesIO(file_bytes), engine=engine)
    names = xl.sheet_names
    sheet_11 = next((n for n in names if "1.1" in n), None)
    results = []
    first_non_empty = None
    for n in names:
        count, passed, err = sheet_has_content_diagnose(file_bytes, n, engine)
        results.append((n, count, passed, err))
        if passed and first_non_empty is None:
            first_non_empty = n
    if sheet_11:
        chosen = sheet_11
        is_multi = True
    else:
        chosen = first_non_empty
        is_multi = False
    return chosen, is_multi, results


def main():
    argv = [a for a in sys.argv[1:] if a != "--out"]
    write_out = "--out" in sys.argv[1:]
    if not argv:
        # 未传参时尝试项目下常见示例目录
        base = Path(__file__).resolve().parent.parent
        for d in base.iterdir():
            if d.is_dir() and "12" in d.name and not d.name.startswith("."):
                path = d
                break
        else:
            path = None
        if path is None:
            print("用法: python scripts/diagnose_sheet_content.py [--out] [文件或目录路径]")
            print("示例: python scripts/diagnose_sheet_content.py 示例原始数据示例12月")
            sys.exit(1)
    else:
        path = Path(argv[0])
    if not path.exists():
        print(f"路径不存在: {path}")
        sys.exit(1)

    files = []
    if path.is_file():
        if path.suffix.lower() in (".xlsx", ".xls"):
            files.append(path)
        else:
            print("请指定 .xlsx 或 .xls 文件")
            sys.exit(1)
    else:
        for f in path.iterdir():
            if f.suffix.lower() in (".xlsx", ".xls"):
                files.append(f)

    if not files:
        print(f"未找到 Excel 文件: {path}")
        sys.exit(0)

    engine = "openpyxl"
    lines = []
    def out(s):
        lines.append(s)
        print(s)

    for f in sorted(files):
        out("\n" + "=" * 70)
        out(f"文件: {f.name}")
        out("=" * 70)
        file_bytes = f.read_bytes()
        try:
            chosen, is_multi, results = find_target_sheet_diagnose(file_bytes, engine)
            for sheet_name, count, passed, err in results:
                has_11 = "1.1" in sheet_name
                status = "通过" if passed else "未通过"
                if err:
                    out(f"  Sheet: {sheet_name!r}")
                    out(f"    含1.1: {has_11}  非空数: 异常  {status}  异常: {err}")
                else:
                    out(f"  Sheet: {sheet_name!r}")
                    out(f"    含1.1: {has_11}  前5行非空单元格数: {count}  >=3: {passed}  ({status})")
            out(f"  -> _find_target_sheet 结论: 选中={chosen!r}, 多Sheet模式={is_multi}")
            if chosen is None:
                out("  -> 因此会报「未找到可用的工作表」")
        except Exception as e:
            out(f"  读取文件失败: {e}")
            import traceback
            out(traceback.format_exc())
    out("")

    if write_out:
        out_path = Path(__file__).resolve().parent.parent / "diagnose_sheet_result.txt"
        with open(out_path, "w", encoding="utf-8") as fp:
            fp.write("\n".join(lines))
        print(f"结果已写入: {out_path}")


if __name__ == "__main__":
    main()
