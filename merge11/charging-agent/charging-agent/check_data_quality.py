# -*- coding: utf-8 -*-
"""
数据质量检查脚本
检查evdata表的数据质量问题
"""
import sys
import os
if os.name == 'nt':
    os.system('chcp 65001 >nul 2>&1')
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

from utils.db_utils import get_shared_engine
from sqlalchemy import text

def main():
    engine = get_shared_engine()
    
    print("=" * 80)
    print("  evdata 表数据质量检查报告")
    print("=" * 80)
    
    with engine.connect() as conn:
        # 1. 获取表结构
        print("\n【1. 表结构概览】")
        result = conn.execute(text('DESCRIBE evdata'))
        columns = []
        for row in result:
            columns.append({
                'name': row[0],
                'type': row[1],
                'null': row[2],
                'key': row[3],
                'default': row[4]
            })
        print(f"总字段数: {len(columns)}")
        
        # 2. 获取总记录数
        total = conn.execute(text("SELECT COUNT(*) FROM evdata")).scalar()
        print(f"\n【2. 数据规模】")
        print(f"总记录数: {total:,}")
        
        # 3. 关键字段空值检查
        print(f"\n【3. 关键字段空值/异常检查】")
        key_fields = [
            ('运营商名称', '运营商'),
            ('省份_中文', '省份'),
            ('城市_中文', '城市'),
            ('区县_中文', '区县'),
            ('充电桩类型_转换', '充电桩类型'),
            ('额定功率', '额定功率'),
            ('所属充电站编号', '充电站编号'),
            ('充电站名称', '充电站名称'),
            ('充电站位置', '充电站位置'),
            ('充电桩编号', '充电桩编号'),
        ]
        
        print(f"{'字段':<20} {'空值/NULL数':<15} {'空字符串数':<15} {'占比':<10}")
        print("-" * 60)
        
        for field, label in key_fields:
            try:
                null_count = conn.execute(text(f"SELECT COUNT(*) FROM evdata WHERE `{field}` IS NULL")).scalar()
                empty_count = conn.execute(text(f"SELECT COUNT(*) FROM evdata WHERE `{field}` = ''")).scalar()
                pct = (null_count + empty_count) / total * 100 if total > 0 else 0
                status = "⚠️" if pct > 5 else "✓"
                print(f"{label:<20} {null_count:<15,} {empty_count:<15,} {pct:.1f}% {status}")
            except Exception as e:
                print(f"{label:<20} 查询失败: {str(e)[:30]}")
        
        # 4. 区县字段异常检查（直辖市问题）
        print(f"\n【4. 区县字段异常检查（直辖市）】")
        direct_cities = ['北京市', '上海市', '天津市', '重庆市']
        for city in direct_cities:
            # 区县=城市名的数量（异常情况）
            abnormal = conn.execute(text(f"""
                SELECT COUNT(*) FROM evdata 
                WHERE 省份_中文 = '{city}' AND 区县_中文 = '{city}'
            """)).scalar()
            total_city = conn.execute(text(f"SELECT COUNT(*) FROM evdata WHERE 省份_中文 = '{city}'")).scalar()
            if total_city > 0:
                pct = abnormal / total_city * 100
                status = "⚠️ 需修复" if pct > 50 else "✓"
                print(f"{city}: 区县异常 {abnormal:,}/{total_city:,} ({pct:.1f}%) {status}")
        
        # 5. 运营商名称不一致检查
        print(f"\n【5. 运营商名称一致性检查】")
        result = conn.execute(text("""
            SELECT 运营商名称, COUNT(*) as cnt 
            FROM evdata 
            WHERE 运营商名称 IS NOT NULL AND 运营商名称 != ''
            GROUP BY 运营商名称 
            ORDER BY cnt DESC 
            LIMIT 20
        """))
        operators = result.fetchall()
        print(f"运营商总数: {len(operators)} (显示Top 20)")
        
        # 检查相似运营商名称
        similar_groups = []
        op_names = [op[0] for op in operators]
        for i, name1 in enumerate(op_names):
            for name2 in op_names[i+1:]:
                # 简单相似度检查：一个是另一个的子串
                if name1 in name2 or name2 in name1:
                    if len(name1) > 2 and len(name2) > 2:
                        similar_groups.append((name1, name2))
        
        if similar_groups:
            print(f"\n可能重复的运营商名称（{len(similar_groups)}组）:")
            for n1, n2 in similar_groups[:10]:
                print(f"  - '{n1}' <-> '{n2}'")
        
        # 6. 数值字段异常检查
        print(f"\n【6. 数值字段异常检查】")
        
        # 额定功率
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN 额定功率 IS NULL THEN 1 ELSE 0 END) as null_cnt,
                SUM(CASE WHEN 额定功率 = 0 THEN 1 ELSE 0 END) as zero_cnt,
                SUM(CASE WHEN 额定功率 < 0 THEN 1 ELSE 0 END) as neg_cnt,
                SUM(CASE WHEN 额定功率 > 1000 THEN 1 ELSE 0 END) as huge_cnt,
                MIN(额定功率) as min_val,
                MAX(额定功率) as max_val,
                AVG(额定功率) as avg_val
            FROM evdata
        """)).fetchone()
        print(f"额定功率:")
        print(f"  - NULL值: {result[1]:,}")
        print(f"  - 零值: {result[2]:,}")
        print(f"  - 负值: {result[3]:,}")
        print(f"  - 超大值(>1000kW): {result[4]:,}")
        print(f"  - 范围: {result[5]} ~ {result[6]} kW")
        print(f"  - 平均值: {result[7]:.1f} kW" if result[7] else "  - 平均值: N/A")
        
        # 7. 日期字段检查
        print(f"\n【7. 日期字段检查】")
        date_fields = ['入库时间', '充电桩生产日期', '设备开通时间']
        for field in date_fields:
            try:
                result = conn.execute(text(f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN `{field}` IS NULL THEN 1 ELSE 0 END) as null_cnt,
                        MIN(`{field}`) as min_date,
                        MAX(`{field}`) as max_date
                    FROM evdata
                """)).fetchone()
                null_pct = result[1] / result[0] * 100 if result[0] > 0 else 0
                print(f"{field}: NULL {result[1]:,} ({null_pct:.1f}%), 范围: {result[2]} ~ {result[3]}")
            except Exception as e:
                print(f"{field}: 查询失败")
        
        # 8. 充电桩类型分布
        print(f"\n【8. 充电桩类型分布】")
        result = conn.execute(text("""
            SELECT 充电桩类型_转换, COUNT(*) as cnt 
            FROM evdata 
            GROUP BY 充电桩类型_转换 
            ORDER BY cnt DESC
        """))
        for row in result:
            type_name = row[0] if row[0] else '(空)'
            print(f"  {type_name}: {row[1]:,}")
        
        # 9. 重复数据检查
        print(f"\n【9. 重复数据检查】")
        
        # 充电桩编号重复
        dup_pile = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT 充电桩编号 FROM evdata 
                WHERE 充电桩编号 IS NOT NULL AND 充电桩编号 != ''
                GROUP BY 充电桩编号 HAVING COUNT(*) > 1
            ) t
        """)).scalar()
        print(f"充电桩编号重复: {dup_pile:,} 个编号存在重复")
        
        # UID重复
        dup_uid = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT UID FROM evdata 
                WHERE UID IS NOT NULL AND UID != ''
                GROUP BY UID HAVING COUNT(*) > 1
            ) t
        """)).scalar()
        print(f"UID重复: {dup_uid:,} 个UID存在重复")
        
    engine.dispose()
    print("\n" + "=" * 80)
    print("检查完成")

if __name__ == '__main__':
    main()
