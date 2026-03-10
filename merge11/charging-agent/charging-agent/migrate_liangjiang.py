"""
重庆区域行政区划调整迁移脚本

江北区（500105）和渝北区（500112）已并入两江新区（500157）
此脚本将更新数据库中的相关记录

使用方法:
    python migrate_liangjiang.py --dry-run   # 预览变更（不实际修改）
    python migrate_liangjiang.py --execute   # 执行变更
"""

import sys
import argparse
from sqlalchemy import create_engine, text

# 数据库连接配置
try:
    from config import DB_CONFIG
    DB_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
except ImportError:
    print("错误: 无法导入数据库配置，请确保 config.py 存在")
    sys.exit(1)

# 区划调整映射
MIGRATION_MAP = {
    '500105': ('江北区', '500157', '两江新区'),    # 原江北区 -> 两江新区
    '500112': ('渝北区', '500157', '两江新区'),    # 原渝北区 -> 两江新区
}


def check_affected_records(engine):
    """检查受影响的记录数"""
    print("\n" + "=" * 60)
    print("检查受影响的数据记录")
    print("=" * 60)
    
    total_affected = 0
    
    with engine.connect() as conn:
        for old_code, (old_name, new_code, new_name) in MIGRATION_MAP.items():
            # 统计受影响的记录数 - 使用反引号转义中文列名
            result = conn.execute(text(
                "SELECT COUNT(*) as cnt FROM evdata WHERE `区县` = :old_code AND is_active = 1"
            ), {"old_code": int(old_code)})
            count = result.fetchone()[0]
            total_affected += count
            
            print(f"  {old_name}({old_code}) -> {new_name}({new_code}): {count} 条记录")
    
    print("-" * 60)
    print(f"  总计需要迁移: {total_affected} 条记录")
    print("=" * 60)
    
    return total_affected


def execute_migration(engine, dry_run=True):
    """执行迁移"""
    if dry_run:
        print("\n[预览模式] 以下是将要执行的变更:")
    else:
        print("\n[执行模式] 正在执行变更...")
    
    total_updated = 0
    
    with engine.connect() as conn:
        for old_code, (old_name, new_code, new_name) in MIGRATION_MAP.items():
            if dry_run:
                # 预览模式：只显示SQL
                print(f"""
  UPDATE evdata 
  SET `区县` = {new_code}, 
      `区县_中文` = '{new_name}'
  WHERE `区县` = {old_code} AND is_active = 1;
                """)
            else:
                # 执行模式：实际更新
                result = conn.execute(text(
                    "UPDATE evdata SET `区县` = :new_code, `区县_中文` = :new_name "
                    "WHERE `区县` = :old_code AND is_active = 1"
                ), {
                    "new_code": int(new_code),
                    "new_name": new_name,
                    "old_code": int(old_code)
                })
                updated = result.rowcount
                total_updated += updated
                print(f"  {old_name}({old_code}) -> {new_name}({new_code}): 更新了 {updated} 条记录")
        
        if not dry_run:
            conn.commit()
            print(f"\n迁移完成！共更新 {total_updated} 条记录")
    
    return total_updated


def verify_migration(engine):
    """验证迁移结果"""
    print("\n" + "=" * 60)
    print("验证迁移结果")
    print("=" * 60)
    
    with engine.connect() as conn:
        # 检查旧编码是否还有数据
        for old_code, (old_name, _, _) in MIGRATION_MAP.items():
            result = conn.execute(text(
                "SELECT COUNT(*) as cnt FROM evdata WHERE `区县` = :old_code AND is_active = 1"
            ), {"old_code": int(old_code)})
            count = result.fetchone()[0]
            
            if count > 0:
                print(f"  警告: {old_name}({old_code}) 仍有 {count} 条活跃记录")
            else:
                print(f"  {old_name}({old_code}): 已完全迁移")
        
        # 检查新编码的数据
        result = conn.execute(text(
            "SELECT COUNT(*) as cnt FROM evdata WHERE `区县` = 500157 AND is_active = 1"
        ))
        new_count = result.fetchone()[0]
        print(f"\n  两江新区(500157): 当前有 {new_count} 条活跃记录")
    
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='重庆区域行政区划调整迁移脚本')
    parser.add_argument('--dry-run', action='store_true', help='预览变更，不实际执行')
    parser.add_argument('--execute', action='store_true', help='执行变更')
    args = parser.parse_args()
    
    if not args.dry_run and not args.execute:
        parser.print_help()
        print("\n请指定 --dry-run（预览）或 --execute（执行）")
        return
    
    print("=" * 60)
    print("重庆区域行政区划调整迁移")
    print("=" * 60)
    print("区划调整说明:")
    print("  - 江北区（500105）并入 两江新区（500157）")
    print("  - 渝北区（500112）并入 两江新区（500157）")
    
    try:
        engine = create_engine(DB_URL)
        
        # 检查受影响的记录
        affected = check_affected_records(engine)
        
        if affected == 0:
            print("\n没有需要迁移的数据")
            return
        
        # 执行迁移
        if args.execute:
            confirm = input("\n确定要执行迁移吗？(输入 'yes' 确认): ")
            if confirm.lower() != 'yes':
                print("已取消")
                return
        
        execute_migration(engine, dry_run=args.dry_run)
        
        # 验证结果（仅执行模式）
        if args.execute:
            verify_migration(engine)
            
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
