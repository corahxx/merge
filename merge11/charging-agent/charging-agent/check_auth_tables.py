# check_auth_tables.py - 检查账号管理体系数据库表
# -*- coding: utf-8 -*-

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from utils.db_utils import get_shared_engine
from sqlalchemy import text

def check_auth_tables():
    engine = get_shared_engine()
    tables_to_check = ['sys_users', 'sys_roles', 'sys_audit_logs', 'sys_login_logs']
    
    print('='*50)
    print('Account Management Database Table Check')
    print('='*50)
    
    with engine.connect() as conn:
        # 获取所有表
        result = conn.execute(text('SHOW TABLES'))
        existing_tables = [row[0] for row in result]
        
        print(f'\nTotal tables in database: {len(existing_tables)}\n')
        
        print('Auth tables status:')
        print('-'*40)
        
        missing_tables = []
        for table in tables_to_check:
            if table in existing_tables:
                # 查询记录数
                count_result = conn.execute(text(f'SELECT COUNT(*) FROM `{table}`'))
                count = count_result.scalar()
                print(f'  [OK] {table}: exists ({count} records)')
            else:
                print(f'  [X]  {table}: NOT CREATED')
                missing_tables.append(table)
        
        print('-'*40)
        
        # 检查 evdata 表的 updated_by 字段
        print('\nevdata.updated_by field check:')
        try:
            result = conn.execute(text("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'evdata' AND COLUMN_NAME = 'updated_by'
            """))
            row = result.fetchone()
            if row:
                print(f'  [OK] updated_by exists: {row[1]}, nullable={row[2]}')
            else:
                print(f'  [X]  updated_by field NOT EXISTS')
        except Exception as e:
            print(f'  [!]  Check failed: {e}')
        
        print('\n' + '='*50)
        
        # 总结
        if missing_tables:
            print(f'\n[!] Missing {len(missing_tables)} tables, auth system NOT deployed')
            print('Tables to create:', ', '.join(missing_tables))
        else:
            print('\n[OK] All auth tables ready')

if __name__ == '__main__':
    check_auth_tables()
