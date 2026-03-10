#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""清空 evdata 表。默认先备份再清空；加 --no-backup 则直接清空不备份。"""

import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG


def main():
    parser = argparse.ArgumentParser(description='清空 evdata 表')
    parser.add_argument('--no-backup', action='store_true', help='不备份，直接清空表')
    args = parser.parse_args()

    print("连接数据库...")
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        autocommit=True,
        connect_timeout=10,
        read_timeout=300
    )

    cursor = conn.cursor()

    # 查询当前记录数
    print("查询当前记录数...")
    cursor.execute('SELECT COUNT(*) FROM evdata')
    count = cursor.fetchone()[0]
    print(f'当前 evdata 表记录数: {count:,}')

    if count == 0:
        print("表已经是空的，无需操作")
        cursor.close()
        conn.close()
        return

    if not args.no_backup:
        # 先备份（小数据量场景下，用于快速回滚）
        from datetime import datetime
        backup_table = f"evdata_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"正在创建备份表 `{backup_table}` ...")
        cursor.execute(f"CREATE TABLE `{backup_table}` AS SELECT * FROM `evdata`")
        cursor.execute(f"SELECT COUNT(*) FROM `{backup_table}`")
        backup_count = cursor.fetchone()[0]
        print(f"备份完成：`{backup_table}` 记录数 {backup_count:,}")
    else:
        print("已指定 --no-backup，跳过备份")

    # 执行清空
    print("正在执行 TRUNCATE TABLE evdata ...")
    cursor.execute('TRUNCATE TABLE evdata')
    cursor.execute('SELECT COUNT(*) FROM evdata')
    after = cursor.fetchone()[0]
    print(f'evdata 表已清空，当前记录数: {after:,}')

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
