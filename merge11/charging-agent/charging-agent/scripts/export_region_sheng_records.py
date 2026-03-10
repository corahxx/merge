#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""导出城市_中文或区县_中文包含"省"字的错误数据到 JSON（充电站位置为空的不输出但统计）。"""

import sys
import os
import json

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
        read_timeout=120,
    )
    cursor = conn.cursor()

    # 查询城市_中文或区县_中文包含「省」的记录
    cursor.execute("""
        SELECT UID, 省份, 城市, 区县, 充电站位置
        FROM evdata
        WHERE 城市_中文 LIKE '%省%' OR 区县_中文 LIKE '%省%'
        ORDER BY 省份, 城市, 区县
    """)
    rows = cursor.fetchall()

    records = []
    empty_location_count = 0

    for uid, province, city, district, location in rows:
        loc = (location or '').strip()
        if not loc or loc.lower() in ('nan', 'none', 'null'):
            empty_location_count += 1
            continue
        records.append({
            'UID': uid,
            '省份': province,
            '城市': city,
            '区县': district,
            '充电站位置': location
        })

    out_path = os.path.join(os.path.dirname(__file__), 'region_sheng_error_records.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print("=" * 60)
    print(f"总查询记录数: {len(rows):,} 条")
    print(f"充电站位置为空（未导出）: {empty_location_count:,} 条")
    print(f"已导出（充电站位置非空）: {len(records):,} 条")
    print("=" * 60)
    print(f"输出文件: {out_path}")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
