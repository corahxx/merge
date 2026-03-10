#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""统计 evdata 中「城市_中文」或「区县_中文」包含"省"字的数据量（全表）。"""

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
        read_timeout=60,
    )
    cursor = conn.cursor()

    # 城市_中文 包含「省」
    cursor.execute("""
        SELECT 省份_中文, COUNT(*) AS cnt
        FROM evdata
        WHERE 城市_中文 LIKE '%省%'
        GROUP BY 省份_中文
        ORDER BY cnt DESC
    """)
    city_rows = cursor.fetchall()

    # 区县_中文 包含「省」
    cursor.execute("""
        SELECT 省份_中文, COUNT(*) AS cnt
        FROM evdata
        WHERE 区县_中文 LIKE '%省%'
        GROUP BY 省份_中文
        ORDER BY cnt DESC
    """)
    dist_rows = cursor.fetchall()

    print("=" * 60)
    print("城市_中文 包含「省」字的记录（按省份分组）：")
    print("-" * 60)
    city_total = 0
    for prov, cnt in city_rows:
        city_total += cnt
        print(f"  {prov or '(空)'}: {cnt:,} 条")
    print("-" * 60)
    print(f"  城市_中文 合计: {city_total:,} 条")

    print()
    print("=" * 60)
    print("区县_中文 包含「省」字的记录（按省份分组）：")
    print("-" * 60)
    dist_total = 0
    for prov, cnt in dist_rows:
        dist_total += cnt
        print(f"  {prov or '(空)'}: {cnt:,} 条")
    print("-" * 60)
    print(f"  区县_中文 合计: {dist_total:,} 条")

    print()
    print("=" * 60)
    print(f"总计（城市或区县含「省」）: {city_total + dist_total:,} 条")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
