# 检查并清理MySQL阻塞进程
# 用法: 从项目根目录运行 python scripts/check_mysql.py
# 或进入scripts目录: cd scripts && python check_mysql.py
import sys
import os

# 添加父目录到路径以便导入config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import DB_CONFIG

conn = pymysql.connect(
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    database=DB_CONFIG['database'],
    charset='utf8mb4'
)

cursor = conn.cursor()

print("=== 当前MySQL进程 ===")
cursor.execute("SHOW FULL PROCESSLIST")
processes = cursor.fetchall()
for p in processes:
    print(f"ID: {p[0]}, User: {p[1]}, DB: {p[3]}, Time: {p[5]}s, State: {p[6]}, Info: {str(p[7])[:60] if p[7] else 'None'}")

print(f"\n总连接数: {len(processes)}")

# 查找长时间运行的查询
print("\n=== 长时间运行的查询 (>10秒) ===")
long_running = [p for p in processes if p[5] and int(p[5]) > 10 and p[4] != 'Sleep']
for p in long_running:
    print(f"  ID: {p[0]}, Time: {p[5]}s, Command: {p[4]}, Info: {str(p[7])[:100] if p[7] else 'None'}")

if long_running:
    print(f"\n发现 {len(long_running)} 个长时间运行的查询")
    response = input("是否终止这些进程? (y/n): ").strip().lower()
    if response == 'y':
        for p in long_running:
            try:
                cursor.execute(f"KILL {p[0]}")
                print(f"  已终止进程 {p[0]}")
            except Exception as e:
                print(f"  终止进程 {p[0]} 失败: {e}")
        conn.commit()
else:
    print("\n没有发现长时间运行的查询")

# 检查InnoDB锁
print("\n=== InnoDB锁状态 ===")
try:
    cursor.execute("SELECT * FROM information_schema.innodb_locks")
    locks = cursor.fetchall()
    print(f"当前锁: {len(locks)}")
except:
    print("无法查询innodb_locks (可能是MySQL 8.0)")

cursor.close()
conn.close()
