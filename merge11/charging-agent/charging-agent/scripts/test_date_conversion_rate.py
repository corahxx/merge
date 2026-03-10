#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 evdata 备份表抽样 充电站投入使用时间，用 DataCleaner._standardize_date_column 测转换率。
供验证日期清洗规则优化效果。
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG
import pandas as pd
from data.data_cleaner import DataCleaner


def main():
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4',
        read_timeout=120,
    )
    cursor = conn.cursor()

    # 取一个 evdata_backup_* 表
    cursor.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE 'evdata_backup_%%'
        ORDER BY TABLE_NAME DESC LIMIT 1
    """, (DB_CONFIG['database'],))
    row = cursor.fetchone()
    if not row:
        print("未找到 evdata_backup_* 表")
        cursor.close()
        conn.close()
        return

    tbl = row[0]
    cursor.execute(f"SELECT `充电站投入使用时间` FROM `{tbl}` LIMIT 100000")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    values = [r[0] for r in rows]
    df = pd.DataFrame({'充电站投入使用时间': values, 'data_status': 0})

    cleaner = DataCleaner(verbose=False, table_name='evdata')
    df2 = cleaner._standardize_date_column(df, '充电站投入使用时间')

    st = cleaner.cleaning_stats['date_standardization']
    parsed = st.get('parsed', 0)
    failed = st.get('failed', 0)
    total = parsed + failed
    non_null = df['充电站投入使用时间'].notna() & (df['充电站投入使用时间'].astype(str).str.strip() != '') & ~df['充电站投入使用时间'].astype(str).str.lower().isin(['nan','none','null','nat'])
    total_non_empty = int(non_null.sum())

    print(f"备份表: {tbl}  抽样: {len(values):,} 行")
    print(f"非空 充电站投入使用时间: {total_non_empty:,}")
    print(f"解析成功: {parsed:,}  解析失败: {failed:,}")
    if total_non_empty:
        rate = 100.0 * parsed / total_non_empty
        print(f"转换率: {rate:.1f}%")


if __name__ == '__main__':
    main()
