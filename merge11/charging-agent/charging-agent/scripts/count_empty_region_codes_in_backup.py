#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""统计 evdata 备份表（400万+）中 省份、城市、区县 编码为空的情况。"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG


def main():
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4',
        connect_timeout=10,
        read_timeout=300,
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE 'evdata_backup_%%'
        ORDER BY TABLE_NAME DESC
    """, (DB_CONFIG['database'],))
    backups = [r[0] for r in cursor.fetchall()]
    if not backups:
        print("未找到 evdata_backup_* 表")
        cursor.close()
        conn.close()
        return

    # 选记录数最多的备份表（400多万那条）
    best = None
    best_cnt = 0
    for t in backups:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{t}`")
            c = int(cursor.fetchone()[0] or 0)
            if c > best_cnt:
                best_cnt = c
                best = t
        except Exception:
            pass
    if not best:
        best = backups[0]
        cursor.execute(f"SELECT COUNT(*) FROM `{best}`")
        best_cnt = int(cursor.fetchone()[0] or 0)

    total = best_cnt
    print(f"备份表: {best}")
    print(f"总行数: {total:,}\n")

    # 1) 省份编码为空
    cursor.execute(f"""
        SELECT COUNT(*) FROM `{best}`
        WHERE `省份` IS NULL OR COALESCE(TRIM(CAST(`省份` AS CHAR)), '') = ''
    """)
    empty_province = int(cursor.fetchone()[0] or 0)

    # 2) 城市编码为空
    cursor.execute(f"""
        SELECT COUNT(*) FROM `{best}`
        WHERE `城市` IS NULL OR COALESCE(TRIM(CAST(`城市` AS CHAR)), '') = ''
    """)
    empty_city = int(cursor.fetchone()[0] or 0)

    # 3) 区县编码为空
    cursor.execute(f"""
        SELECT COUNT(*) FROM `{best}`
        WHERE `区县` IS NULL OR COALESCE(TRIM(CAST(`区县` AS CHAR)), '') = ''
    """)
    empty_district = int(cursor.fetchone()[0] or 0)

    # 4) 三省/市/区县都为空
    cursor.execute(f"""
        SELECT COUNT(*) FROM `{best}`
        WHERE (`省份` IS NULL OR COALESCE(TRIM(CAST(`省份` AS CHAR)), '') = '')
          AND (`城市` IS NULL OR COALESCE(TRIM(CAST(`城市` AS CHAR)), '') = '')
          AND (`区县` IS NULL OR COALESCE(TRIM(CAST(`区县` AS CHAR)), '') = '')
    """)
    empty_all_three = int(cursor.fetchone()[0] or 0)

    # 5) 至少有一个编码为空
    cursor.execute(f"""
        SELECT COUNT(*) FROM `{best}`
        WHERE (`省份` IS NULL OR COALESCE(TRIM(CAST(`省份` AS CHAR)), '') = '')
           OR (`城市` IS NULL OR COALESCE(TRIM(CAST(`城市` AS CHAR)), '') = '')
           OR (`区县` IS NULL OR COALESCE(TRIM(CAST(`区县` AS CHAR)), '') = '')
    """)
    empty_any = int(cursor.fetchone()[0] or 0)

    # 6) 三省/市/区县都非空（可直接用 JSON 编码映射）
    non_empty_all = total - empty_any

    lines = [
        "=== 省份/城市/区县 编码为空 统计 ===",
        f"{'指标':<32} {'数量':>12} {'占比':>10}",
        "-" * 56,
        f"{'省份 为空':<32} {empty_province:>12,} {100.0*empty_province/total:>9.2f}%" if total else "",
        f"{'城市 为空':<32} {empty_city:>12,} {100.0*empty_city/total:>9.2f}%" if total else "",
        f"{'区县 为空':<32} {empty_district:>12,} {100.0*empty_district/total:>9.2f}%" if total else "",
        f"{'三省/市/区县 都为空':<32} {empty_all_three:>12,} {100.0*empty_all_three/total:>9.2f}%" if total else "",
        f"{'至少一个编码为空':<32} {empty_any:>12,} {100.0*empty_any/total:>9.2f}%" if total else "",
        "-" * 56,
        f"{'三省/市/区县 都非空（可完全用编码映射）':<32} {non_empty_all:>12,} {100.0*non_empty_all/total:>9.2f}%" if total else "",
    ]
    out_path = os.path.join(os.path.dirname(__file__), 'count_empty_region_codes_output.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"备份表: {best}\n总行数: {total:,}\n\n" + "\n".join(lines) + "\n")
    for line in lines:
        print(line)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
