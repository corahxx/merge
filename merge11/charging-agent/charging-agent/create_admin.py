# create_admin.py - 创建默认管理员账号
# -*- coding: utf-8 -*-

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from utils.db_utils import get_shared_engine
from sqlalchemy import text

def create_admin():
    engine = get_shared_engine()
    
    # bcrypt hash for "Admin@2026"
    password_hash = '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4bEjGHLrI9VJL2Gy'
    
    with engine.connect() as conn:
        # Check if admin exists
        result = conn.execute(text("SELECT COUNT(*) FROM sys_users WHERE username='admin'"))
        count = result.scalar()
        
        if count > 0:
            print('[!] Admin user already exists')
            # Show current admin info
            result = conn.execute(text("SELECT username, display_name, role, status FROM sys_users WHERE username='admin'"))
            row = result.fetchone()
            if row:
                print(f'    Username: {row[0]}')
                print(f'    Display Name: {row[1]}')
                print(f'    Role: {row[2]}')
                print(f'    Status: {row[3]}')
        else:
            # Insert admin
            conn.execute(text("""
                INSERT INTO sys_users (username, password_hash, display_name, role, status) 
                VALUES ('admin', :pwd_hash, 'System Admin', 'admin', 'active')
            """), {'pwd_hash': password_hash})
            conn.commit()
            
            print('='*50)
            print('[OK] Admin user created successfully!')
            print('='*50)
            print('')
            print('  Username: admin')
            print('  Password: Admin@2026')
            print('')
            print('[!] Please change password after first login!')
            print('='*50)

if __name__ == '__main__':
    create_admin()
