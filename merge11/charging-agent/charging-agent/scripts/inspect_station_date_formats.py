#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 evdata 备份表中抽样 充电站投入使用时间（字符串），统计格式分布，供日期清洗规则优化。
"""

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
        read_timeout=120,
    )
    cursor = conn.cursor()

    # 查找备份表（evdata_backup_%）
    cursor.execute("""
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE 'evdata_backup_%%'
        ORDER BY TABLE_NAME DESC
    """, (DB_CONFIG['database'],))
    backups = [r[0] for r in cursor.fetchall()]
    if not backups:
        print("未找到 evdata_backup_* 表，请先有备份表。")
        cursor.close()
        conn.close()
        return

    # 选记录数最多的备份表
    best = None
    best_cnt = 0
    for t in backups:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{t}`")
            c = cursor.fetchone()[0]
            if c > best_cnt:
                best_cnt = c
                best = t
        except Exception:
            pass
    if not best:
        best = backups[0]
        cursor.execute(f"SELECT COUNT(*) FROM `{best}`")
        best_cnt = cursor.fetchone()[0]

    print(f"使用备份表: {best} (约 {best_cnt:,} 行)")
    print()

    # 检查是否有 充电站投入使用时间 列
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = '充电站投入使用时间'
    """, (DB_CONFIG['database'], best))
    col = cursor.fetchone()
    if not col:
        print("备份表中无 充电站投入使用时间 列")
        cursor.close()
        conn.close()
        return

    # 总量与非空
    cursor.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN `充电站投入使用时间` IS NOT NULL AND TRIM(`充电站投入使用时间`) != '' THEN 1 ELSE 0 END) AS non_empty
        FROM `{best}`
    """)
    total, non_empty = cursor.fetchone()
    total = int(total or 0)
    non_empty = int(non_empty or 0)
    print(f"总行数: {total:,}  非空 充电站投入使用时间: {non_empty:,}")
    print()

    # 按值分组统计，取前 300 个最常见值
    cursor.execute(f"""
        SELECT `充电站投入使用时间`, COUNT(*) AS cnt
        FROM `{best}`
        WHERE `充电站投入使用时间` IS NOT NULL AND TRIM(CAST(`充电站投入使用时间` AS CHAR)) != ''
        GROUP BY `充电站投入使用时间`
        ORDER BY cnt DESC
        LIMIT 400
    """)
    rows = cursor.fetchall()

    out_path = os.path.join(os.path.dirname(__file__), 'inspect_station_date_formats_output.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"备份表: {best}  总行数: {total:,}  非空: {non_empty:,}\n\n")
        f.write("=== 充电站投入使用时间 按值分布 (前400) ===\n")
        f.write(f"{'值':<60} {'数量':>12} {'累计%':>8}\n")
        f.write("-" * 82 + "\n")
        cum = 0
        for val, cnt in rows:
            cnt = int(cnt or 0)
            cum += cnt
            pct = 100.0 * cum / non_empty if non_empty else 0
            v = str(val) if val is not None else ''
            if len(v) > 58:
                v = v[:56] + '..'
            f.write(f"{v:<60} {cnt:>12,} {pct:>7.1f}%\n")
        f.write("\n")

    print("=== 充电站投入使用时间 按值分布 (前400) ===")
    print(f"已写入 {out_path}")
    cum = 0
    for val, cnt in rows[:60]:
        cum += int(cnt or 0)
        pct = 100.0 * cum / non_empty if non_empty else 0
        v = (str(val)[:48] + '..') if len(str(val)) > 50 else str(val)
        print(f"{v:<50} {cnt:>12,} {pct:>7.1f}%")

    # 再抽样一批随机值，看是否有 GROUP BY 里没覆盖的格式
    cursor.execute(f"""
        SELECT `充电站投入使用时间` FROM `{best}`
        WHERE `充电站投入使用时间` IS NOT NULL AND TRIM(CAST(`充电站投入使用时间` AS CHAR)) != ''
        ORDER BY RAND()
        LIMIT 2000
    """)
    samples = [r[0] for r in cursor.fetchall()]
    seen_in_top = {str(r[0]) for r in rows}
    extra = [s for s in samples if str(s) not in seen_in_top]
    if extra:
        from collections import Counter
        ex = Counter(str(x) for x in extra)
        print()
        print("=== 随机抽样中未出现在上表的值（示例，最多50） ===")
        for v, _ in ex.most_common(50):
            print(f"  {v[:70]}")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
