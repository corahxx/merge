#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""导出 evdata 中「湖南省」「河北省」且「城市_中文」包含"省"字的记录到 JSON。"""

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
        read_timeout=60,
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT UID, 省份, 城市, 区县, 充电站位置
        FROM evdata
        WHERE 省份_中文 IN ('湖南省', '河北省')
          AND 城市_中文 LIKE '%省%'
        ORDER BY 省份, 城市, 区县
    """)
    rows = cursor.fetchall()

    records = []
    for uid, province, city, district, location in rows:
        records.append({
            'UID': uid,
            '省份': province,
            '城市': city,
            '区县': district,
            '充电站位置': location
        })

    out_path = os.path.join(os.path.dirname(__file__), 'city_sheng_records.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"已导出 {len(records):,} 条记录到: {out_path}")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
