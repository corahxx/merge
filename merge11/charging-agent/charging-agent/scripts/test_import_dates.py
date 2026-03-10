#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
导入测试：充电站投入使用时间 DATE 格式与日期转换、区域名称同步。
使用 scripts/test_import_data.csv，经 DataProcessor 导入 evdata 后查询验证。
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from data.data_processor import DataProcessor
from sqlalchemy import text
from utils.db_utils import create_db_engine
from config import DB_CONFIG


def main():
    root = Path(__file__).resolve().parents[1]
    csv_path = root / 'scripts' / 'test_import_data.csv'
    if not csv_path.exists():
        print(f"测试数据不存在: {csv_path}")
        return

    print("=" * 60)
    print("1. 导入测试数据 (DataProcessor -> evdata)")
    print("=" * 60)
    processor = DataProcessor(table_name='evdata', verbose=False)
    result = processor.process_excel_file(str(csv_path), if_exists='append')
    if result.get('status') != 'success':
        print("导入失败:", result.get('errors', result))
        return
    print("导入成功, rows_loaded:", result.get('rows_loaded', 0))

    print("\n" + "=" * 60)
    print("2. 验证 充电站投入使用时间 类型与样例")
    print("=" * 60)
    engine = create_db_engine(echo=False)
    with engine.connect() as conn:
        # 列类型
        r = conn.execute(text("""
            SELECT COLUMN_TYPE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'evdata' AND COLUMN_NAME = '充电站投入使用时间'
        """), {"db": DB_CONFIG["database"]}).fetchone()
        if r:
            print("列类型: %s (DATA_TYPE=%s)" % (r[0], r[1]))
        # 样例：按充电桩编号前缀筛选本次测试
        rows = conn.execute(text("""
            SELECT 充电桩编号, 充电站投入使用时间, 省份_中文, 城市_中文, 区县_中文, data_status
            FROM evdata WHERE 充电桩编号 LIKE 'TEST-DATE-%%' ORDER BY 充电桩编号
        """)).fetchall()
        print("样例 (TEST-DATE-*):")
        for row in rows:
            print("  ", row)

    print("\n" + "=" * 60)
    print("3. data_status 分布 (TEST-DATE-*)")
    print("=" * 60)
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT data_status, COUNT(*) as cnt FROM evdata WHERE 充电桩编号 LIKE 'TEST-DATE-%%' GROUP BY data_status
        """)).fetchall()
        for row in r:
            print("  data_status=%s: %s" % (row[0], row[1]))

    print("\n" + "=" * 60)
    print("4. 日期为 NULL 的条数 (应为空或 bad-date 等解析失败)")
    print("=" * 60)
    with engine.connect() as conn:
        n = conn.execute(text("""
            SELECT COUNT(*) FROM evdata WHERE 充电桩编号 LIKE 'TEST-DATE-%%' AND 充电站投入使用时间 IS NULL
        """)).scalar()
        print("  充电站投入使用时间 IS NULL: %s" % n)

    print("\n导入与验证完成。")


if __name__ == '__main__':
    main()
