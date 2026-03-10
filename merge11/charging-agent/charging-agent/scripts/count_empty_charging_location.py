#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""统计 evdata 备份表中 充电站位置 为空的记录数。"""

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

    # 选记录数最多的备份表（如 400 多万条的那张）
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

    cursor.execute(f"""
        SELECT COUNT(*) FROM `{best}`
        WHERE `充电站位置` IS NULL OR TRIM(IFNULL(CAST(`充电站位置` AS CHAR), '')) = ''
    """)
    empty = int(cursor.fetchone()[0] or 0)

    print(f"备份表: {best}")
    print(f"总行数: {total:,}")
    print(f"充电站位置为空: {empty:,}")
    print(f"占比: {100.0 * empty / total:.2f}%" if total else "占比: -")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
