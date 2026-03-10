# fix_admin_password.py - 修复admin密码
# -*- coding: utf-8 -*-

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from utils.db_utils import get_shared_engine
from sqlalchemy import text
import bcrypt

def fix_admin_password():
    engine = get_shared_engine()
    
    with engine.connect() as conn:
        # 检查当前密码哈希
        result = conn.execute(text("SELECT username, password_hash FROM sys_users WHERE username='admin'"))
        row = result.fetchone()
        
        if row:
            print(f'Username: {row[0]}')
            print(f'Current hash: {row[1][:50]}...')
            print()
            
            # 生成新的正确哈希
            password = 'Admin@2026'
            new_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt(rounds=12))
            new_hash_str = new_hash.decode('utf-8')
            
            print(f'New hash: {new_hash_str[:50]}...')
            
            # 验证新哈希
            verify_result = bcrypt.checkpw(password.encode('utf-8'), new_hash)
            print(f'Verify new hash: {verify_result}')
            
            # 更新密码
            conn.execute(text("UPDATE sys_users SET password_hash = :hash WHERE username = 'admin'"), 
                        {'hash': new_hash_str})
            conn.commit()
            
            print()
            print('='*50)
            print('Password updated successfully!')
            print('='*50)
            print()
            print('  Username: admin')
            print('  Password: Admin@2026')
            print()
        else:
            print('Admin user not found')

if __name__ == '__main__':
    fix_admin_password()
