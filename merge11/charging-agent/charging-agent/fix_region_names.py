# fix_region_names.py - 修复数据库中的区域中文名称（生产级版本）
"""
根据区域代码映射文件修复数据库中的省份_中文、城市_中文、区县_中文字段
支持断点续传、进度保存、分批提交

使用方法:
  预览模式: python fix_region_names.py
  执行修复: python fix_region_names.py --execute
  继续修复: python fix_region_names.py --execute --resume
"""

import json
import os
import sys
import time
from datetime import datetime
from sqlalchemy import create_engine, text

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_CONFIG

# 进度文件路径
PROGRESS_FILE = os.path.join(os.path.dirname(__file__), '.fix_region_progress.json')


def load_region_mapping():
    """加载区域代码映射"""
    mapping_file = os.path.join(os.path.dirname(__file__), 'data', 'region_code_mapping.json')
    
    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"区域代码映射文件不存在: {mapping_file}")
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return {
        'provinces': data.get('省份映射', {}),
        'cities': data.get('城市映射', {}),
        'districts': data.get('区县映射', {})
    }


def get_engine():
    """创建数据库连接"""
    connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
    return create_engine(connection_string)


def load_progress():
    """加载进度"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        'completed_fields': [],  # 已完成的字段
        'current_field': None,   # 当前处理的字段
        'processed_codes': [],   # 当前字段已处理的代码
        'stats': {
            'province_fixed': 0,
            'city_fixed': 0,
            'district_fixed': 0
        }
    }


def save_progress(progress):
    """保存进度"""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def clear_progress():
    """清除进度文件"""
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("已清除进度文件")


def fix_region_names(dry_run=True, resume=False, batch_size=100, commit_interval=500):
    """
    修复数据库中的区域中文名称
    
    Args:
        dry_run: 如果为True，只显示将要修改的内容，不实际执行
        resume: 是否从上次中断的地方继续
        batch_size: 每批处理的代码数（用于SQL构建）
        commit_interval: 每处理多少条记录后提交一次事务
    """
    print("="*60)
    print("区域中文名称修复脚本 (生产级版本)")
    print(f"执行模式: {'预览模式 (不修改数据)' if dry_run else '实际修复模式'}")
    print(f"断点续传: {'是' if resume else '否'}")
    print(f"批次大小: {batch_size} 代码/批")
    print(f"提交间隔: {commit_interval} 条/次")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 加载或初始化进度
    progress = load_progress() if resume else {
        'completed_fields': [],
        'current_field': None,
        'processed_codes': [],
        'stats': {'province_fixed': 0, 'city_fixed': 0, 'district_fixed': 0}
    }
    
    if resume and progress['completed_fields']:
        print(f"\n从上次进度继续，已完成字段: {progress['completed_fields']}")
    
    # 加载映射
    print("\n[1/4] 加载区域代码映射...")
    mapping = load_region_mapping()
    print(f"  - 省份: {len(mapping['provinces'])} 条")
    print(f"  - 城市: {len(mapping['cities'])} 条")
    print(f"  - 区县: {len(mapping['districts'])} 条")
    
    # 连接数据库
    print("\n[2/4] 连接数据库...")
    engine = get_engine()
    
    stats = progress['stats']
    
    with engine.connect() as conn:
        # 获取总记录数
        result = conn.execute(text("SELECT COUNT(*) FROM evdata WHERE is_active = 1"))
        total_records = result.scalar()
        print(f"  - 活跃记录总数: {total_records:,}")
        
        # 定义要处理的字段
        fields_to_process = [
            ('省份', '省份_中文', mapping['provinces'], 'province_fixed'),
            ('城市', '城市_中文', mapping['cities'], 'city_fixed'),
            ('区县', '区县_中文', mapping['districts'], 'district_fixed')
        ]
        
        # 分析需要修复的数据
        print("\n[3/4] 分析需要修复的数据...")
        
        analysis_results = {}
        for code_field, name_field, field_mapping, stat_key in fields_to_process:
            if code_field in progress['completed_fields']:
                print(f"  {name_field}: 已在上次运行中完成，跳过分析")
                continue
            
            print(f"  分析 {name_field}...")
            to_fix = analyze_field(conn, field_mapping, code_field, name_field)
            analysis_results[code_field] = to_fix
            print(f"    - 需要修复: {len(to_fix)} 种代码")
        
        # 显示示例
        if '区县' in analysis_results and analysis_results['区县']:
            print("\n区县修复示例 (最多显示10条):")
            show_examples(analysis_results['区县'][:10], '区县')
        
        if dry_run:
            total_to_fix = sum(len(v) for v in analysis_results.values())
            print("\n" + "="*60)
            print("预览模式完成 - 未修改任何数据")
            print(f"共发现 {total_to_fix} 种代码需要修复")
            print("如需实际执行修复，请运行: python fix_region_names.py --execute")
            print("="*60)
            return stats
        
        # 实际执行修复
        print("\n[4/4] 执行修复...")
        start_time = time.time()
        
        for code_field, name_field, field_mapping, stat_key in fields_to_process:
            # 跳过已完成的字段
            if code_field in progress['completed_fields']:
                print(f"\n  {name_field}: 已完成，跳过")
                continue
            
            if code_field not in analysis_results or not analysis_results[code_field]:
                print(f"\n  {name_field}: 无需修复")
                progress['completed_fields'].append(code_field)
                save_progress(progress)
                continue
            
            to_fix = analysis_results[code_field]
            print(f"\n  修复 {name_field} ({len(to_fix)} 种代码)...")
            
            # 设置当前处理字段
            progress['current_field'] = code_field
            
            # 获取已处理的代码（用于断点续传）
            processed_codes = set(progress['processed_codes']) if progress['current_field'] == code_field else set()
            
            # 过滤掉已处理的
            remaining = [(c, curr, corr) for c, curr, corr in to_fix if str(c) not in processed_codes]
            
            if len(remaining) < len(to_fix):
                print(f"    从断点继续: 已跳过 {len(to_fix) - len(remaining)} 种已处理的代码")
            
            # 分批处理
            fixed_count = 0
            for i in range(0, len(remaining), batch_size):
                batch = remaining[i:i + batch_size]
                
                try:
                    batch_fixed = fix_batch(conn, batch, code_field, name_field)
                    fixed_count += batch_fixed
                    
                    # 记录已处理的代码
                    for code, _, _ in batch:
                        processed_codes.add(str(code))
                    
                    # 提交事务
                    conn.commit()
                    
                    # 更新并保存进度
                    progress['processed_codes'] = list(processed_codes)
                    stats[stat_key] = fixed_count
                    progress['stats'] = stats
                    save_progress(progress)
                    
                    # 显示进度
                    elapsed = time.time() - start_time
                    progress_pct = (i + len(batch)) / len(remaining) * 100
                    print(f"    进度: {i + len(batch)}/{len(remaining)} ({progress_pct:.1f}%) | "
                          f"已修复: {fixed_count:,} 条 | 耗时: {elapsed:.1f}s")
                    
                except Exception as e:
                    print(f"    批次处理错误: {e}")
                    conn.rollback()
                    # 继续下一批
                    continue
            
            # 标记字段完成
            progress['completed_fields'].append(code_field)
            progress['current_field'] = None
            progress['processed_codes'] = []
            save_progress(progress)
            
            print(f"    {name_field} 完成: 修复 {fixed_count:,} 条记录")
    
    # 清除进度文件
    clear_progress()
    
    # 打印统计
    total_time = time.time() - start_time
    print("\n" + "="*60)
    print("修复完成!")
    print(f"  总记录数: {total_records:,}")
    print(f"  省份修复: {stats['province_fixed']:,} 条")
    print(f"  城市修复: {stats['city_fixed']:,} 条")
    print(f"  区县修复: {stats['district_fixed']:,} 条")
    total_fixed = stats['province_fixed'] + stats['city_fixed'] + stats['district_fixed']
    print(f"  总计修复: {total_fixed:,} 条")
    print(f"  总耗时: {total_time:.1f} 秒")
    if total_fixed > 0:
        print(f"  平均速度: {total_fixed / total_time:.0f} 条/秒")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    return stats


def analyze_field(conn, mapping, code_field, name_field):
    """
    分析某个字段需要修复的记录
    返回需要修复的 (code, current_name, correct_name) 列表
    """
    to_fix = []
    
    # 查询所有不同的 (code, name) 组合
    query = text(f"""
        SELECT DISTINCT `{code_field}`, `{name_field}` 
        FROM evdata 
        WHERE is_active = 1 AND `{code_field}` IS NOT NULL AND `{code_field}` != ''
    """)
    
    result = conn.execute(query)
    
    for row in result:
        code = str(row[0]).strip() if row[0] else ''
        current_name = str(row[1]).strip() if row[1] else ''
        
        if not code:
            continue
        
        # 确保代码格式正确用于查询映射
        lookup_code = code
        if code_field == '省份' and len(code) == 2:
            lookup_code = code + "0000"
        elif code_field == '城市' and len(code) == 4:
            lookup_code = code + "00"
        elif code_field == '区县' and len(code) < 6:
            lookup_code = code.zfill(6)
        
        correct_name = mapping.get(lookup_code, '')
        
        # 如果正确名称存在且与当前名称不同，则需要修复
        if correct_name and correct_name != current_name:
            to_fix.append((row[0], current_name, correct_name))  # 使用原始代码
    
    return to_fix


def show_examples(to_fix_list, field_name):
    """显示需要修复的示例"""
    if not to_fix_list:
        print(f"  {field_name}: 无需修复")
        return
    
    print(f"  {field_name}:")
    for code, current, correct in to_fix_list:
        print(f"    代码 {code}: '{current}' -> '{correct}'")


def fix_batch(conn, batch, code_field, name_field):
    """
    修复一批数据
    返回修复的记录数
    """
    if not batch:
        return 0
    
    total_fixed = 0
    
    # 对每个代码单独执行更新（更可靠）
    for original_code, current_name, correct_name in batch:
        try:
            query = text(f"""
                UPDATE evdata 
                SET `{name_field}` = :correct_name
                WHERE is_active = 1 
                AND `{code_field}` = :code
            """)
            
            result = conn.execute(query, {
                'correct_name': correct_name,
                'code': original_code
            })
            total_fixed += result.rowcount
        except Exception as e:
            print(f"      更新代码 {original_code} 失败: {e}")
            continue
    
    return total_fixed


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='修复数据库中的区域中文名称')
    parser.add_argument('--execute', action='store_true', 
                        help='实际执行修复（默认为预览模式）')
    parser.add_argument('--resume', action='store_true',
                        help='从上次中断的地方继续')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='每批处理的代码数（默认50）')
    parser.add_argument('--clear-progress', action='store_true',
                        help='清除进度文件并退出')
    
    args = parser.parse_args()
    
    if args.clear_progress:
        clear_progress()
        return
    
    try:
        stats = fix_region_names(
            dry_run=not args.execute,
            resume=args.resume,
            batch_size=args.batch_size
        )
        
    except KeyboardInterrupt:
        print("\n\n用户中断！进度已保存。")
        print("下次运行时使用 --resume 参数可以继续: python fix_region_names.py --execute --resume")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        print("\n进度已保存，下次运行时使用 --resume 参数可以继续")
        sys.exit(2)


if __name__ == '__main__':
    main()
