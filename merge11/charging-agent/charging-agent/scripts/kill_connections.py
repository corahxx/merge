# 强制终止阻塞的MySQL连接
# 用法: 从项目根目录运行 python scripts/kill_connections.py
# 或进入scripts目录: cd scripts && python kill_connections.py
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

# 获取当前连接ID
cursor.execute("SELECT CONNECTION_ID()")
my_id = cursor.fetchone()[0]
print(f"当前连接ID: {my_id}")

# 获取所有evdata相关的连接
cursor.execute("SHOW FULL PROCESSLIST")
processes = cursor.fetchall()

killed = 0
for p in processes:
    pid = p[0]
    db = p[3]
    time_sec = p[5]
    state = p[6]
    
    # 跳过自己、event_scheduler
    if pid == my_id or p[1] == 'event_scheduler':
        continue
    
    # 终止所有evdata数据库的连接
    if db == 'evdata':
        try:
            cursor.execute(f"KILL {pid}")
            print(f"已终止: ID={pid}, Time={time_sec}s, State={state}")
            killed += 1
        except Exception as e:
            print(f"终止失败: ID={pid}, Error={e}")

print(f"\n共终止 {killed} 个连接")

cursor.close()
conn.close()
print("完成!")
