#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""查询 evdata 中 充电站投入使用时间 的填充率（有日期/总行数）。"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG


def main():
    c = pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4',
    )
    cur = c.cursor()

    cur.execute("SELECT COUNT(*) FROM evdata")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM evdata WHERE `充电站投入使用时间` IS NOT NULL")
    has_date = cur.fetchone()[0]

    cur.close()
    c.close()

    total = int(total or 0)
    has_date = int(has_date or 0)
    rate = 100.0 * has_date / total if total else 0

    print("evdata 总行数:", f"{total:,}")
    print("充电站投入使用时间 非 NULL:", f"{has_date:,}")
    print("转换率(有日期/总行数):", f"{rate:.1f}%")


if __name__ == "__main__":
    main()
