# -*- coding: utf-8 -*-
"""
数据库SQL优化执行脚本
- 自动创建索引
- 自动创建视图
- 支持错误回滚
- 详细日志输出

使用方法：
    python execute_db_optimization.py [--dry-run] [--indexes-only] [--views-only]

参数：
    --dry-run      只显示要执行的SQL，不实际执行
    --indexes-only 只执行索引创建
    --views-only   只执行视图创建
    --rollback     回滚已创建的索引和视图

作者：VDBP
日期：2026-01-13
"""

import sys
import argparse
from datetime import datetime
from typing import List, Tuple, Optional

# 添加项目路径
sys.path.insert(0, '.')

from config import DB_CONFIG
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ============================================
# 配置区
# ============================================

# 单列索引定义 (索引名, 字段名, 前缀长度)
# 前缀长度: None=不需要, 数字=TEXT类型字段需要指定前缀长度
SINGLE_INDEXES = [
    ("idx_operator_name", "运营商名称", None),
    ("idx_province", "省份_中文", None),
    ("idx_city", "城市_中文", None),
    ("idx_district", "区县_中文", 100),  # TEXT类型，需要前缀长度
    ("idx_rated_power", "额定功率", None),
    ("idx_pile_type", "充电桩类型_转换", None),
    ("idx_station_code", "所属充电站编号", None),
    ("idx_production_date", "充电桩生产日期", None),
    ("idx_import_time", "入库时间", None),
    ("idx_pile_code", "充电桩编号", None),
    ("idx_pile_model", "充电桩型号", None),
]

# 复合索引定义 (索引名, 字段列表)
# 字段格式: "字段名" 或 ("字段名", 前缀长度) 用于TEXT类型
COMPOSITE_INDEXES = [
    ("idx_region_operator", ["省份_中文", "城市_中文", "运营商名称"]),
    ("idx_region_hierarchy", ["省份_中文", "城市_中文", ("区县_中文", 100)]),  # 区县_中文是TEXT类型
    ("idx_operator_type", ["运营商名称", "充电桩类型_转换"]),
    ("idx_power_type", ["额定功率", "充电桩类型_转换"]),
]

# 视图定义 (视图名, SQL)
VIEWS = [
    ("v_operator_stats", """
        SELECT 
            运营商名称,
            COUNT(*) as 充电桩数量,
            COUNT(DISTINCT 所属充电站编号) as 充电站数量,
            ROUND(AVG(额定功率), 2) as 平均功率
        FROM evdata
        WHERE 运营商名称 IS NOT NULL
        GROUP BY 运营商名称
    """),
    ("v_region_stats", """
        SELECT 
            省份_中文,
            城市_中文,
            区县_中文,
            COUNT(*) as 充电桩数量,
            COUNT(DISTINCT 所属充电站编号) as 充电站数量
        FROM evdata
        WHERE 省份_中文 IS NOT NULL
        GROUP BY 省份_中文, 城市_中文, 区县_中文
    """),
    ("v_power_distribution", """
        SELECT 
            CASE 
                WHEN 额定功率 <= 7 THEN '≤7kW（慢充）'
                WHEN 额定功率 <= 30 THEN '7-30kW（小功率）'
                WHEN 额定功率 <= 60 THEN '30-60kW（中功率）'
                WHEN 额定功率 <= 120 THEN '60-120kW（大功率）'
                ELSE '>120kW（超快充）'
            END as 功率区间,
            COUNT(*) as 数量
        FROM evdata
        WHERE 额定功率 IS NOT NULL
        GROUP BY 功率区间
    """),
    ("v_pile_type_stats", """
        SELECT 
            充电桩类型_转换,
            COUNT(*) as 数量,
            ROUND(AVG(额定功率), 2) as 平均功率
        FROM evdata
        WHERE 充电桩类型_转换 IS NOT NULL
        GROUP BY 充电桩类型_转换
    """),
]


# ============================================
# 工具函数
# ============================================

def log(message: str, level: str = "INFO"):
    """日志输出"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    icons = {
        "INFO": "ℹ️ ",
        "SUCCESS": "✅",
        "ERROR": "❌",
        "WARNING": "⚠️ ",
        "STEP": "🔧",
        "ROLLBACK": "🔄",
    }
    icon = icons.get(level, "  ")
    print(f"[{timestamp}] {icon} {message}")


def get_db_url():
    """获取数据库连接URL"""
    return (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )


def create_db_connection():
    """创建数据库连接"""
    try:
        engine = create_engine(
            get_db_url(),
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
        )
        # 测试连接
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        log(f"数据库连接失败: {e}", "ERROR")
        return None


def get_existing_indexes(conn) -> List[str]:
    """获取已存在的索引名列表"""
    result = conn.execute(text("""
        SELECT INDEX_NAME 
        FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'evdata'
        GROUP BY INDEX_NAME
    """), {"db": DB_CONFIG['database']})
    return [row[0] for row in result.fetchall()]


def get_existing_views(conn) -> List[str]:
    """获取已存在的视图名列表"""
    result = conn.execute(text("""
        SELECT TABLE_NAME 
        FROM information_schema.VIEWS 
        WHERE TABLE_SCHEMA = :db
    """), {"db": DB_CONFIG['database']})
    return [row[0] for row in result.fetchall()]


# ============================================
# 索引操作
# ============================================

def create_single_index(conn, index_name: str, column_name: str, prefix_length: Optional[int] = None, dry_run: bool = False) -> Tuple[bool, str]:
    """创建单列索引"""
    if prefix_length:
        # TEXT类型字段需要指定前缀长度
        sql = f"CREATE INDEX `{index_name}` ON evdata (`{column_name}`({prefix_length}))"
    else:
        sql = f"CREATE INDEX `{index_name}` ON evdata (`{column_name}`)"
    
    if dry_run:
        log(f"[DRY-RUN] {sql}", "STEP")
        return True, sql
    
    try:
        conn.execute(text(sql))
        conn.commit()
        log(f"创建索引 {index_name} 成功", "SUCCESS")
        return True, sql
    except SQLAlchemyError as e:
        error_msg = str(e)
        if "Duplicate key name" in error_msg:
            log(f"索引 {index_name} 已存在，跳过", "WARNING")
            return True, ""  # 已存在不算失败
        else:
            log(f"创建索引 {index_name} 失败: {error_msg}", "ERROR")
            return False, sql


def create_composite_index(conn, index_name: str, columns: List, dry_run: bool = False) -> Tuple[bool, str]:
    """创建复合索引"""
    # 处理字段定义：可以是字符串 或 (字段名, 前缀长度) 元组
    col_parts = []
    for c in columns:
        if isinstance(c, tuple):
            col_name, prefix_len = c
            col_parts.append(f"`{col_name}`({prefix_len})")
        else:
            col_parts.append(f"`{c}`")
    cols_str = ", ".join(col_parts)
    sql = f"CREATE INDEX `{index_name}` ON evdata ({cols_str})"
    
    if dry_run:
        log(f"[DRY-RUN] {sql}", "STEP")
        return True, sql
    
    try:
        conn.execute(text(sql))
        conn.commit()
        log(f"创建复合索引 {index_name} 成功", "SUCCESS")
        return True, sql
    except SQLAlchemyError as e:
        error_msg = str(e)
        if "Duplicate key name" in error_msg:
            log(f"索引 {index_name} 已存在，跳过", "WARNING")
            return True, ""
        else:
            log(f"创建复合索引 {index_name} 失败: {error_msg}", "ERROR")
            return False, sql


def drop_index(conn, index_name: str, dry_run: bool = False) -> bool:
    """删除索引"""
    sql = f"DROP INDEX `{index_name}` ON evdata"
    
    if dry_run:
        log(f"[DRY-RUN] {sql}", "ROLLBACK")
        return True
    
    try:
        conn.execute(text(sql))
        conn.commit()
        log(f"删除索引 {index_name} 成功", "ROLLBACK")
        return True
    except SQLAlchemyError as e:
        log(f"删除索引 {index_name} 失败: {e}", "ERROR")
        return False


# ============================================
# 视图操作
# ============================================

def create_view(conn, view_name: str, view_sql: str, dry_run: bool = False) -> Tuple[bool, str]:
    """创建或替换视图"""
    sql = f"CREATE OR REPLACE VIEW `{view_name}` AS {view_sql}"
    
    if dry_run:
        log(f"[DRY-RUN] CREATE VIEW {view_name}", "STEP")
        return True, sql
    
    try:
        conn.execute(text(sql))
        conn.commit()
        log(f"创建视图 {view_name} 成功", "SUCCESS")
        return True, sql
    except SQLAlchemyError as e:
        log(f"创建视图 {view_name} 失败: {e}", "ERROR")
        return False, sql


def drop_view(conn, view_name: str, dry_run: bool = False) -> bool:
    """删除视图"""
    sql = f"DROP VIEW IF EXISTS `{view_name}`"
    
    if dry_run:
        log(f"[DRY-RUN] {sql}", "ROLLBACK")
        return True
    
    try:
        conn.execute(text(sql))
        conn.commit()
        log(f"删除视图 {view_name} 成功", "ROLLBACK")
        return True
    except SQLAlchemyError as e:
        log(f"删除视图 {view_name} 失败: {e}", "ERROR")
        return False


# ============================================
# 主执行逻辑
# ============================================

def execute_indexes(conn, dry_run: bool = False) -> Tuple[int, int, List[str]]:
    """
    执行所有索引创建
    返回: (成功数, 失败数, 已创建的索引列表)
    """
    log("=" * 50)
    log("开始创建索引...")
    log("=" * 50)
    
    success_count = 0
    fail_count = 0
    created_indexes = []
    
    # 获取已存在的索引
    existing = get_existing_indexes(conn)
    log(f"当前已有 {len(existing)} 个索引")
    
    # 创建单列索引
    log("\n--- 单列索引 ---")
    for idx_name, col_name, prefix_len in SINGLE_INDEXES:
        if idx_name in existing:
            log(f"索引 {idx_name} 已存在，跳过", "WARNING")
            continue
            
        success, sql = create_single_index(conn, idx_name, col_name, prefix_len, dry_run)
        if success:
            success_count += 1
            if sql:  # 只有实际创建的才加入列表
                created_indexes.append(idx_name)
        else:
            fail_count += 1
            if not dry_run:
                # 失败时回滚已创建的索引
                log(f"\n检测到失败，开始回滚已创建的 {len(created_indexes)} 个索引...", "ROLLBACK")
                for idx in created_indexes:
                    drop_index(conn, idx)
                return success_count, fail_count, []
    
    # 创建复合索引
    log("\n--- 复合索引 ---")
    for idx_name, cols in COMPOSITE_INDEXES:
        if idx_name in existing:
            log(f"索引 {idx_name} 已存在，跳过", "WARNING")
            continue
            
        success, sql = create_composite_index(conn, idx_name, cols, dry_run)
        if success:
            success_count += 1
            if sql:
                created_indexes.append(idx_name)
        else:
            fail_count += 1
            if not dry_run:
                log(f"\n检测到失败，开始回滚已创建的 {len(created_indexes)} 个索引...", "ROLLBACK")
                for idx in created_indexes:
                    drop_index(conn, idx)
                return success_count, fail_count, []
    
    return success_count, fail_count, created_indexes


def execute_views(conn, dry_run: bool = False) -> Tuple[int, int, List[str]]:
    """
    执行所有视图创建
    返回: (成功数, 失败数, 已创建的视图列表)
    """
    log("=" * 50)
    log("开始创建视图...")
    log("=" * 50)
    
    success_count = 0
    fail_count = 0
    created_views = []
    
    for view_name, view_sql in VIEWS:
        success, sql = create_view(conn, view_name, view_sql, dry_run)
        if success:
            success_count += 1
            created_views.append(view_name)
        else:
            fail_count += 1
            if not dry_run:
                log(f"\n检测到失败，开始回滚已创建的 {len(created_views)} 个视图...", "ROLLBACK")
                for vw in created_views:
                    drop_view(conn, vw)
                return success_count, fail_count, []
    
    return success_count, fail_count, created_views


def execute_rollback(conn, dry_run: bool = False):
    """回滚：删除所有创建的索引和视图"""
    log("=" * 50)
    log("开始回滚操作...")
    log("=" * 50)
    
    # 回滚索引
    log("\n--- 回滚索引 ---")
    existing_indexes = get_existing_indexes(conn)
    # SINGLE_INDEXES 是三元组 (名称, 字段, 前缀长度)
    # COMPOSITE_INDEXES 是二元组 (名称, 字段列表)
    all_index_names = [idx[0] for idx in SINGLE_INDEXES] + [idx[0] for idx in COMPOSITE_INDEXES]
    
    for idx_name in all_index_names:
        if idx_name in existing_indexes:
            drop_index(conn, idx_name, dry_run)
        else:
            log(f"索引 {idx_name} 不存在，跳过", "WARNING")
    
    # 回滚视图
    log("\n--- 回滚视图 ---")
    for view_name, _ in VIEWS:
        drop_view(conn, view_name, dry_run)
    
    log("\n回滚完成！", "SUCCESS")


def show_current_status(conn):
    """显示当前数据库状态"""
    log("=" * 50)
    log("当前数据库状态")
    log("=" * 50)
    
    # 显示现有索引
    indexes = get_existing_indexes(conn)
    log(f"\n📊 已有索引 ({len(indexes)} 个):")
    for idx in indexes:
        log(f"   - {idx}")
    
    # 显示现有视图
    views = get_existing_views(conn)
    log(f"\n📋 已有视图 ({len(views)} 个):")
    for vw in views:
        log(f"   - {vw}")
    
    # 显示表记录数
    result = conn.execute(text("SELECT COUNT(*) FROM evdata"))
    count = result.scalar()
    log(f"\n📈 evdata 表记录数: {count:,}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="数据库SQL优化执行脚本")
    parser.add_argument("--dry-run", action="store_true", help="只显示要执行的SQL，不实际执行")
    parser.add_argument("--indexes-only", action="store_true", help="只执行索引创建")
    parser.add_argument("--views-only", action="store_true", help="只执行视图创建")
    parser.add_argument("--rollback", action="store_true", help="回滚已创建的索引和视图")
    parser.add_argument("--status", action="store_true", help="只显示当前数据库状态")
    args = parser.parse_args()
    
    print()
    log("=" * 50)
    log("数据库SQL优化执行脚本 v1.0")
    log("=" * 50)
    
    if args.dry_run:
        log("模式: DRY-RUN (只显示SQL，不执行)", "WARNING")
    
    # 连接数据库
    log(f"\n连接数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    engine = create_db_connection()
    if not engine:
        log("无法连接数据库，退出", "ERROR")
        return 1
    
    log("数据库连接成功", "SUCCESS")
    
    try:
        with engine.connect() as conn:
            # 只显示状态
            if args.status:
                show_current_status(conn)
                return 0
            
            # 回滚模式
            if args.rollback:
                confirm = input("\n⚠️  确定要回滚所有优化吗？这将删除所有创建的索引和视图。(y/N): ")
                if confirm.lower() != 'y':
                    log("操作已取消", "WARNING")
                    return 0
                execute_rollback(conn, args.dry_run)
                return 0
            
            # 显示当前状态
            show_current_status(conn)
            
            # 确认执行
            if not args.dry_run:
                print()
                confirm = input("⚠️  确定要执行优化吗？(y/N): ")
                if confirm.lower() != 'y':
                    log("操作已取消", "WARNING")
                    return 0
            
            total_success = 0
            total_fail = 0
            
            # 执行索引
            if not args.views_only:
                idx_success, idx_fail, created_idx = execute_indexes(conn, args.dry_run)
                total_success += idx_success
                total_fail += idx_fail
                
                if idx_fail > 0 and not args.dry_run:
                    log("\n索引创建过程中出现错误，已自动回滚", "ERROR")
                    return 1
            
            # 执行视图
            if not args.indexes_only:
                view_success, view_fail, created_views = execute_views(conn, args.dry_run)
                total_success += view_success
                total_fail += view_fail
                
                if view_fail > 0 and not args.dry_run:
                    log("\n视图创建过程中出现错误，已自动回滚", "ERROR")
                    return 1
            
            # 最终结果
            print()
            log("=" * 50)
            log("执行完成！")
            log("=" * 50)
            log(f"成功: {total_success} | 失败: {total_fail}")
            
            if not args.dry_run:
                log("\n📌 验证方法:")
                log("   1. 运行: python execute_db_optimization.py --status")
                log("   2. 或执行: SHOW INDEX FROM evdata;")
            
            return 0 if total_fail == 0 else 1
            
    except Exception as e:
        log(f"执行过程中发生异常: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        engine.dispose()
        log("\n数据库连接已关闭", "INFO")


if __name__ == "__main__":
    exit(main())
