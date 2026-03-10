#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""统计 evdata 中「湖南省」「河北省」下 城市_中文 包含“省”字的数据量。"""

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

    # 按省份_中文统计：湖南省、河北省 且 城市_中文 包含「省」
    cursor.execute("""
        SELECT 省份_中文, COUNT(*) AS cnt
        FROM evdata
        WHERE 省份_中文 IN ('湖南省', '河北省')
          AND 城市_中文 LIKE '%省%'
        GROUP BY 省份_中文
        ORDER BY 省份_中文
    """)
    rows = cursor.fetchall()

    total = 0
    print("evdata 表中「城市_中文」包含“省”字的记录数（按省）：")
    print("-" * 50)
    for 省份_中文, cnt in rows:
        total += cnt
        print(f"  {省份_中文}: {cnt:,} 条")
    print("-" * 50)
    print(f"  合计（湖南+河北）: {total:,} 条")

    # 顺带给个样本：这些“城市_中文”长什么样
    cursor.execute("""
        SELECT 省份_中文, 城市_中文, COUNT(*) AS cnt
        FROM evdata
        WHERE 省份_中文 IN ('湖南省', '河北省')
          AND 城市_中文 LIKE '%省%'
        GROUP BY 省份_中文, 城市_中文
        ORDER BY 省份_中文, cnt DESC
        LIMIT 20
    """)
    samples = cursor.fetchall()
    if samples:
        print("\n出现较多的「城市_中文」示例（前20种）：")
        for 省, 城市, c in samples:
            print(f"  {省} | {城市!r} : {c:,} 条")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
