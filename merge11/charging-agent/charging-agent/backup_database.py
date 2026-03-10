# -*- coding: utf-8 -*-
"""
数据库备份脚本
支持备份：数据、表结构、索引、视图、存储过程
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import config
    from sqlalchemy import create_engine, text, inspect
    import pandas as pd
except ImportError as e:
    print(f"❌ 缺少依赖: {e}")
    print("请确保已安装: pip install sqlalchemy pymysql pandas")
    sys.exit(1)


def get_db_config():
    """获取数据库配置"""
    return {
        'host': config.DB_CONFIG.get('host', 'localhost'),
        'port': config.DB_CONFIG.get('port', 3306),
        'user': config.DB_CONFIG.get('user', 'root'),
        'password': config.DB_CONFIG.get('password', ''),
        'database': config.DB_CONFIG.get('database', 'evdata')
    }


def get_db_url():
    """获取数据库连接URL"""
    cfg = get_db_config()
    return f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}?charset=utf8mb4"


def create_backup_dir():
    """创建备份目录"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = Path(f"backup_{timestamp}")
    backup_dir.mkdir(exist_ok=True)
    return backup_dir


def try_mysqldump(backup_dir: Path) -> bool:
    """尝试使用 mysqldump 进行完整备份"""
    cfg = get_db_config()
    
    # 检查 mysqldump 是否可用
    try:
        result = subprocess.run(['mysqldump', '--version'], capture_output=True, text=True)
        if result.returncode != 0:
            return False
    except FileNotFoundError:
        return False
    
    print("📦 检测到 mysqldump，使用完整备份模式...")
    
    # 完整备份（结构+数据+视图+存储过程+触发器）
    backup_file = backup_dir / f"{cfg['database']}_full_backup.sql"
    
    cmd = [
        'mysqldump',
        f"--host={cfg['host']}",
        f"--port={cfg['port']}",
        f"--user={cfg['user']}",
        f"--password={cfg['password']}",
        '--routines',           # 包含存储过程和函数
        '--triggers',           # 包含触发器
        '--single-transaction', # 一致性备份
        '--quick',              # 大表优化
        '--set-charset',
        cfg['database']
    ]
    
    print(f"   正在导出到: {backup_file}")
    
    try:
        with open(backup_file, 'w', encoding='utf-8') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            size_mb = backup_file.stat().st_size / (1024 * 1024)
            print(f"✅ mysqldump 备份成功！文件大小: {size_mb:.2f} MB")
            return True
        else:
            print(f"⚠️ mysqldump 出错: {result.stderr}")
            return False
    except Exception as e:
        print(f"⚠️ mysqldump 执行失败: {e}")
        return False


def backup_with_python(backup_dir: Path):
    """使用 Python 进行备份（备用方案）"""
    print("📦 使用 Python 模式备份...")
    
    engine = create_engine(get_db_url(), echo=False)
    cfg = get_db_config()
    
    with engine.connect() as conn:
        # 1. 备份表结构和索引
        print("\n--- 1. 备份表结构和索引 ---")
        schema_file = backup_dir / "01_schema_and_indexes.sql"
        
        with open(schema_file, 'w', encoding='utf-8') as f:
            f.write(f"-- 数据库备份: {cfg['database']}\n")
            f.write(f"-- 备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- 备份类型: 表结构和索引\n\n")
            
            # 获取所有表
            tables = conn.execute(text("SHOW TABLES")).fetchall()
            for (table_name,) in tables:
                print(f"   备份表结构: {table_name}")
                create_sql = conn.execute(text(f"SHOW CREATE TABLE `{table_name}`")).fetchone()
                if create_sql:
                    f.write(f"-- 表: {table_name}\n")
                    f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n")
                    f.write(f"{create_sql[1]};\n\n")
        
        print(f"✅ 表结构已保存到: {schema_file}")
        
        # 2. 备份视图
        print("\n--- 2. 备份视图 ---")
        views_file = backup_dir / "02_views.sql"
        
        with open(views_file, 'w', encoding='utf-8') as f:
            f.write(f"-- 视图备份\n")
            f.write(f"-- 备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            views = conn.execute(text(f"""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.VIEWS 
                WHERE TABLE_SCHEMA = '{cfg['database']}'
            """)).fetchall()
            
            if views:
                for (view_name,) in views:
                    print(f"   备份视图: {view_name}")
                    create_sql = conn.execute(text(f"SHOW CREATE VIEW `{view_name}`")).fetchone()
                    if create_sql:
                        f.write(f"-- 视图: {view_name}\n")
                        f.write(f"DROP VIEW IF EXISTS `{view_name}`;\n")
                        f.write(f"{create_sql[1]};\n\n")
                print(f"✅ 视图已保存到: {views_file}")
            else:
                f.write("-- 没有视图\n")
                print("   没有视图需要备份")
        
        # 3. 备份存储过程
        print("\n--- 3. 备份存储过程 ---")
        procedures_file = backup_dir / "03_procedures.sql"
        
        with open(procedures_file, 'w', encoding='utf-8') as f:
            f.write(f"-- 存储过程备份\n")
            f.write(f"-- 备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            procedures = conn.execute(text(f"""
                SELECT ROUTINE_NAME, ROUTINE_TYPE 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_SCHEMA = '{cfg['database']}'
            """)).fetchall()
            
            if procedures:
                for proc_name, proc_type in procedures:
                    print(f"   备份{proc_type}: {proc_name}")
                    try:
                        create_sql = conn.execute(text(f"SHOW CREATE {proc_type} `{proc_name}`")).fetchone()
                        if create_sql:
                            f.write(f"-- {proc_type}: {proc_name}\n")
                            f.write(f"DROP {proc_type} IF EXISTS `{proc_name}`;\n")
                            f.write("DELIMITER //\n")
                            # 存储过程的创建语句在第3列
                            f.write(f"{create_sql[2]}//\n")
                            f.write("DELIMITER ;\n\n")
                    except Exception as e:
                        f.write(f"-- 无法备份 {proc_type} {proc_name}: {e}\n\n")
                print(f"✅ 存储过程已保存到: {procedures_file}")
            else:
                f.write("-- 没有存储过程\n")
                print("   没有存储过程需要备份")
        
        # 4. 备份索引信息（单独列出）
        print("\n--- 4. 备份索引信息 ---")
        indexes_file = backup_dir / "04_indexes_info.txt"
        
        with open(indexes_file, 'w', encoding='utf-8') as f:
            f.write(f"索引信息报告\n")
            f.write(f"备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n\n")
            
            tables = conn.execute(text("SHOW TABLES")).fetchall()
            for (table_name,) in tables:
                indexes = conn.execute(text(f"SHOW INDEX FROM `{table_name}`")).fetchall()
                if indexes:
                    f.write(f"表: {table_name}\n")
                    f.write("-" * 40 + "\n")
                    
                    # 按索引名分组
                    index_dict = {}
                    for idx in indexes:
                        idx_name = idx[2]  # Key_name
                        col_name = idx[4]  # Column_name
                        non_unique = idx[1]  # Non_unique
                        
                        if idx_name not in index_dict:
                            index_dict[idx_name] = {
                                'columns': [],
                                'unique': non_unique == 0
                            }
                        index_dict[idx_name]['columns'].append(col_name)
                    
                    for idx_name, idx_info in index_dict.items():
                        idx_type = "UNIQUE" if idx_info['unique'] else "INDEX"
                        cols = ", ".join(idx_info['columns'])
                        f.write(f"  {idx_type} {idx_name}: ({cols})\n")
                    f.write("\n")
        
        print(f"✅ 索引信息已保存到: {indexes_file}")
        
        # 5. 备份数据（CSV格式，方便查看和导入）
        print("\n--- 5. 备份表数据 ---")
        data_dir = backup_dir / "data"
        data_dir.mkdir(exist_ok=True)
        
        tables = conn.execute(text("SHOW TABLES")).fetchall()
        for (table_name,) in tables:
            print(f"   备份数据: {table_name}", end="")
            try:
                # 获取行数
                count = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
                print(f" ({count:,} 行)", end="")
                
                if count > 0:
                    # 分批导出大表
                    csv_file = data_dir / f"{table_name}.csv"
                    
                    if count > 100000:
                        # 大表分批导出
                        batch_size = 50000
                        first_batch = True
                        for offset in range(0, count, batch_size):
                            df = pd.read_sql(
                                f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}",
                                conn
                            )
                            df.to_csv(csv_file, mode='a' if not first_batch else 'w', 
                                     index=False, header=first_batch, encoding='utf-8-sig')
                            first_batch = False
                            print(".", end="", flush=True)
                    else:
                        df = pd.read_sql(f"SELECT * FROM `{table_name}`", conn)
                        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                    
                    print(" ✅")
                else:
                    print(" (空表，跳过)")
                    
            except Exception as e:
                print(f" ❌ 错误: {e}")
        
        # 6. 生成 SQL INSERT 语句（可选，用于完整恢复）
        print("\n--- 6. 生成数据恢复SQL ---")
        
        # 只为小表生成 INSERT 语句
        insert_file = backup_dir / "05_data_insert.sql"
        with open(insert_file, 'w', encoding='utf-8') as f:
            f.write(f"-- 数据恢复SQL\n")
            f.write(f"-- 备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- 注意: 仅包含小表(<10000行)的INSERT语句\n")
            f.write(f"-- 大表请使用 data/ 目录下的CSV文件导入\n\n")
            
            tables = conn.execute(text("SHOW TABLES")).fetchall()
            for (table_name,) in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
                
                if 0 < count <= 10000:
                    print(f"   生成INSERT: {table_name} ({count} 行)")
                    f.write(f"-- 表: {table_name} ({count} 行)\n")
                    
                    # 获取数据
                    rows = conn.execute(text(f"SELECT * FROM `{table_name}`")).fetchall()
                    columns = conn.execute(text(f"SHOW COLUMNS FROM `{table_name}`")).fetchall()
                    col_names = [col[0] for col in columns]
                    
                    if rows:
                        # 生成批量INSERT
                        f.write(f"INSERT INTO `{table_name}` (`{'`, `'.join(col_names)}`) VALUES\n")
                        for i, row in enumerate(rows):
                            values = []
                            for val in row:
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                else:
                                    escaped = str(val).replace("'", "''").replace("\\", "\\\\")
                                    values.append(f"'{escaped}'")
                            
                            suffix = "," if i < len(rows) - 1 else ";"
                            f.write(f"({', '.join(values)}){suffix}\n")
                        f.write("\n")
                elif count > 10000:
                    f.write(f"-- 表 {table_name} 数据量较大({count}行)，请使用 data/{table_name}.csv 导入\n\n")
        
        print(f"✅ 数据恢复SQL已保存到: {insert_file}")
    
    engine.dispose()


def main():
    print("=" * 60)
    print("  数据库备份工具")
    print("=" * 60)
    
    cfg = get_db_config()
    print(f"\n数据库: {cfg['database']}@{cfg['host']}:{cfg['port']}")
    
    # 测试连接
    print("\n🔍 测试数据库连接...")
    try:
        engine = create_engine(get_db_url(), echo=False)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        print("✅ 数据库连接成功")
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        sys.exit(1)
    
    # 创建备份目录
    backup_dir = create_backup_dir()
    print(f"\n📁 备份目录: {backup_dir}")
    
    # 尝试使用 mysqldump
    if not try_mysqldump(backup_dir):
        print("\n⚠️ mysqldump 不可用，使用 Python 模式备份")
        backup_with_python(backup_dir)
    
    # 生成备份说明
    readme_file = backup_dir / "README.txt"
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(f"数据库备份说明\n")
        f.write(f"=" * 40 + "\n\n")
        f.write(f"备份时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"数据库: {cfg['database']}\n")
        f.write(f"服务器: {cfg['host']}:{cfg['port']}\n\n")
        f.write(f"文件说明:\n")
        f.write(f"-" * 40 + "\n")
        f.write(f"01_schema_and_indexes.sql - 表结构和索引\n")
        f.write(f"02_views.sql              - 视图定义\n")
        f.write(f"03_procedures.sql         - 存储过程\n")
        f.write(f"04_indexes_info.txt       - 索引详细信息\n")
        f.write(f"05_data_insert.sql        - 小表数据INSERT语句\n")
        f.write(f"data/                     - CSV格式表数据\n")
        f.write(f"\n")
        f.write(f"恢复方法:\n")
        f.write(f"-" * 40 + "\n")
        f.write(f"1. 先执行 01_schema_and_indexes.sql 创建表结构\n")
        f.write(f"2. 执行 02_views.sql 创建视图\n")
        f.write(f"3. 执行 03_procedures.sql 创建存储过程\n")
        f.write(f"4. 执行 05_data_insert.sql 或导入 data/*.csv\n")
    
    print("\n" + "=" * 60)
    print("✅ 备份完成！")
    print("=" * 60)
    print(f"\n📁 备份文件位置: {backup_dir.absolute()}")
    print("\n💡 提示: 请将备份目录复制到安全位置保存")


if __name__ == '__main__':
    main()
