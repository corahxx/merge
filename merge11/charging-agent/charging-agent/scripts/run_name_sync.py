# 区域名称同步脚本 - 使用独立MySQL连接
# 用法: 从项目根目录运行 python -u scripts/run_name_sync.py --yes
# 或进入scripts目录: cd scripts && python -u run_name_sync.py --yes
import sys
import os
import time
import json
import pymysql

# 添加父目录到路径以便导入config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

sys.stdout.reconfigure(line_buffering=True)

def log(msg):
    print(msg, flush=True)

def get_connection():
    """创建新的数据库连接"""
    return pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset='utf8mb4',
        autocommit=True  # 自动提交
    )

def main():
    auto_confirm = '--yes' in sys.argv or '-y' in sys.argv
    
    log("=" * 60)
    log("区域名称同步脚本 (PyMySQL直连)")
    log("=" * 60)
    
    # 获取项目根目录
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_path = os.path.join(project_root, 'data', 'region_code_mapping.json')
    
    # 加载JSON映射
    log("\n[1/4] 加载JSON映射...")
    with open(json_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    
    province_map = mapping.get('省份映射', {})
    city_map = mapping.get('城市映射', {})
    district_map = mapping.get('区县映射', {})
    
    log(f"  - 省份: {len(province_map)}")
    log(f"  - 城市: {len(city_map)}")
    log(f"  - 区县: {len(district_map)}")
    
    if not auto_confirm:
        log("\n" + "=" * 60)
        response = input("是否执行同步? (y/n): ").strip().lower()
        if response != 'y':
            log("已取消。")
            return
    else:
        log("\n自动确认模式，开始执行...")
    
    start_time = time.time()
    batch_size = 2000
    
    # ========== 同步省份名称 ==========
    log("\n[2/4] 同步省份名称...")
    conn = get_connection()
    cursor = conn.cursor()
    province_fixed = 0
    
    for code, json_name in province_map.items():
        code = str(code).zfill(6)[:6]
        try:
            sql = """
                UPDATE evdata SET 省份_中文 = %s
                WHERE is_active = 1 AND 省份 = %s 
                AND 省份_中文 IS NOT NULL AND 省份_中文 != %s
                LIMIT %s
            """
            while True:
                cursor.execute(sql, (json_name, code, json_name, batch_size))
                affected = cursor.rowcount
                if affected == 0:
                    break
                province_fixed += affected
                log(f"    [{code}] {json_name}: +{affected} 条")
        except Exception as e:
            log(f"    [{code}] 错误: {e}")
    
    cursor.close()
    conn.close()
    log(f"  省份完成: {province_fixed:,} 条")
    
    # ========== 同步城市名称 ==========
    log("\n[3/4] 同步城市名称...")
    conn = get_connection()
    cursor = conn.cursor()
    city_fixed = 0
    processed = 0
    
    for i, (code, json_name) in enumerate(city_map.items()):
        code = str(code).zfill(6)[:6]
        try:
            sql = """
                UPDATE evdata SET 城市_中文 = %s
                WHERE is_active = 1 AND 城市 = %s 
                AND 城市_中文 IS NOT NULL AND 城市_中文 != %s
                LIMIT %s
            """
            while True:
                cursor.execute(sql, (json_name, code, json_name, batch_size))
                affected = cursor.rowcount
                if affected == 0:
                    break
                city_fixed += affected
                processed += 1
                if processed <= 20 or processed % 50 == 0:
                    log(f"    [{code}] {json_name}: +{affected} 条")
        except Exception as e:
            log(f"    [{code}] 错误: {e}")
        
        if (i + 1) % 100 == 0:
            elapsed = time.time() - start_time
            log(f"  进度: {i+1}/{len(city_map)} 城市 ({city_fixed:,} 条, {elapsed:.0f}s)")
    
    cursor.close()
    conn.close()
    log(f"  城市完成: {city_fixed:,} 条")
    
    # ========== 同步区县名称 ==========
    log("\n[4/4] 同步区县名称...")
    conn = get_connection()
    cursor = conn.cursor()
    district_fixed = 0
    processed = 0
    
    for i, (code, json_name) in enumerate(district_map.items()):
        code = str(code).zfill(6)[:6]
        try:
            sql = """
                UPDATE evdata SET 区县_中文 = %s
                WHERE is_active = 1 AND 区县 = %s 
                AND 区县_中文 IS NOT NULL AND 区县_中文 != %s
                LIMIT %s
            """
            while True:
                cursor.execute(sql, (json_name, code, json_name, batch_size))
                affected = cursor.rowcount
                if affected == 0:
                    break
                district_fixed += affected
                processed += 1
        except Exception as e:
            pass  # 静默处理
        
        if (i + 1) % 500 == 0:
            elapsed = time.time() - start_time
            log(f"  进度: {i+1}/{len(district_map)} 区县 ({district_fixed:,} 条, {elapsed:.0f}s)")
    
    cursor.close()
    conn.close()
    log(f"  区县完成: {district_fixed:,} 条")
    
    # 完成
    elapsed = time.time() - start_time
    total_fixed = province_fixed + city_fixed + district_fixed
    
    log("\n" + "=" * 60)
    log("同步完成!")
    log(f"  - 省份修复: {province_fixed:,} 条")
    log(f"  - 城市修复: {city_fixed:,} 条")
    log(f"  - 区县修复: {district_fixed:,} 条")
    log(f"  - 总计: {total_fixed:,} 条")
    log(f"  - 耗时: {elapsed:.1f} 秒")
    log("=" * 60)

if __name__ == '__main__':
    main()
