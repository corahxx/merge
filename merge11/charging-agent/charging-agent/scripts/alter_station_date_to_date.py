#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""将 evdata.充电站投入使用时间 从 VARCHAR 改为 DATE。幂等：若已是 DATE 则跳过。"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG


def main():
    print("连接数据库...")
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        autocommit=True,
        connect_timeout=10,
    )
    cursor = conn.cursor()

    # 检查列当前类型
    cursor.execute("""
        SELECT DATA_TYPE, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'evdata' AND COLUMN_NAME = '充电站投入使用时间'
    """, (DB_CONFIG['database'],))
    row = cursor.fetchone()
    if not row:
        print("evdata 中未找到列 充电站投入使用时间，跳过")
        cursor.close()
        conn.close()
        return

    data_type, col_type = row[0], row[1]
    if data_type and str(data_type).upper() == 'DATE':
        print("充电站投入使用时间 已是 DATE 类型，无需修改")
        cursor.close()
        conn.close()
        return

    print(f"当前类型: {col_type}，执行 ALTER 改为 DATE NULL ...")
    cursor.execute("ALTER TABLE evdata MODIFY COLUMN `充电站投入使用时间` DATE NULL")
    print("ALTER 完成")

    # 再次确认
    cursor.execute("""
        SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'evdata' AND COLUMN_NAME = '充电站投入使用时间'
    """, (DB_CONFIG['database'],))
    r = cursor.fetchone()
    if r and str(r[0]).upper() == 'DATE':
        print("已确认: 充电站投入使用时间 为 DATE")
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
