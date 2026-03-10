#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""从 evdata 备份表统计 额定功率 的分布：有哪些值、各有多少。"""

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
        read_timeout=180,
    )
    cursor = conn.cursor()

    # 查找 evdata_backup_*，选记录数最多的
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

    print(f"使用备份表: {best} (约 {best_cnt:,} 行)\n")

    # 总量、非空
    cursor.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN `额定功率` IS NOT NULL AND TRIM(CAST(`额定功率` AS CHAR)) != '' THEN 1 ELSE 0 END) AS non_empty
        FROM `{best}`
    """)
    total, non_empty = cursor.fetchone()
    total = int(total or 0)
    non_empty = int(non_empty or 0)
    print(f"总行数: {total:,}  额定功率 非空: {non_empty:,}\n")

    # 按 额定功率 分组统计
    cursor.execute(f"""
        SELECT `额定功率`, COUNT(*) AS cnt
        FROM `{best}`
        GROUP BY `额定功率`
        ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()

    out_path = os.path.join(os.path.dirname(__file__), 'inspect_power_from_backup_output.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"备份表: {best}  总行数: {total:,}  额定功率非空: {non_empty:,}\n\n")
        f.write("=== 额定功率 分布 ===\n")
        f.write(f"{'额定功率':<30} {'数量':>15} {'占比':>10}\n")
        f.write("-" * 58 + "\n")
        for val, cnt in rows:
            cnt = int(cnt or 0)
            pct = 100.0 * cnt / total if total else 0
            v = str(val) if val is not None else 'NULL'
            if len(v) > 28:
                v = v[:26] + '..'
            f.write(f"{v:<30} {cnt:>15,} {pct:>9.1f}%\n")
        f.write("\n")

    print("=== 额定功率 分布 ===")
    print(f"{'额定功率':<24} {'数量':>12} {'占比':>8}")
    print("-" * 46)
    for val, cnt in rows:
        cnt = int(cnt or 0)
        pct = 100.0 * cnt / total if total else 0
        v = str(val) if val is not None else 'NULL'
        if len(v) > 22:
            v = v[:20] + '..'
        print(f"{v:<24} {cnt:>12,} {pct:>7.1f}%")
    print(f"\n已写入 {out_path}")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
