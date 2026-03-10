#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""从 evdata 备份表导出 UID、省份/城市/区县编码、充电站位置 到 JSON 文件，先导出 10 条供预览。"""

import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG


def _serialize(val):
    """将 DB 值转为 JSON 可序列化：None -> null，其他保持；int/float 保留。"""
    if val is None:
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return val
    return str(val).strip() if isinstance(val, str) else str(val)


def main():
    limit = 10
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

    # 列：UID, 省份(编码), 城市(编码), 区县(编码), 充电站位置
    cursor.execute(f"""
        SELECT `UID`, `省份`, `城市`, `区县`, `充电站位置`
        FROM `{best}`
        LIMIT {limit}
    """)
    rows = cursor.fetchall()
    cols = ['UID', '省份', '城市', '区县', '充电站位置']

    out = []
    for r in rows:
        out.append({k: _serialize(v) for k, v in zip(cols, r)})

    out_path = os.path.join(os.path.dirname(__file__), 'backup_uid_region_sample.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"已导出 {len(out)} 条到: {out_path}")
    print("\n预览:")
    print(json.dumps(out, ensure_ascii=False, indent=2))

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
