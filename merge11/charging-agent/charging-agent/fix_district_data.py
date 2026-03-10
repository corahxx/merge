# -*- coding: utf-8 -*-
"""
修复天津和重庆的区县数据
从充电站位置字段中提取区县名称，更新区县_中文字段
"""

import sys
import os
import time

# 设置控制台UTF-8编码
if os.name == 'nt':
    os.system('chcp 65001 >nul 2>&1')
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

sys.path.insert(0, '.')

from utils.db_utils import create_db_engine
from sqlalchemy import text

# 配置参数
BATCH_SIZE = 500  # 每批更新的记录数
LOCK_TIMEOUT = 120  # 锁等待超时（秒）
BATCH_DELAY = 0.5  # 批次间延迟（秒）

# 天津市的区
TIANJIN_DISTRICTS = [
    '和平区', '河东区', '河西区', '南开区', '河北区', '红桥区',
    '东丽区', '西青区', '津南区', '北辰区', '武清区', '宝坻区',
    '滨海新区', '宁河区', '静海区', '蓟州区'
]

# 重庆市的区县
CHONGQING_DISTRICTS = [
    # 主城区
    '渝中区', '大渡口区', '江北区', '沙坪坝区', '九龙坡区', '南岸区',
    '北碚区', '渝北区', '巴南区',
    # 其他区
    '万州区', '涪陵区', '黔江区', '长寿区', '江津区', '合川区',
    '永川区', '南川区', '綦江区', '大足区', '铜梁区', '璧山区',
    '潼南区', '荣昌区', '开州区', '梁平区', '武隆区',
    # 县
    '城口县', '丰都县', '垫江县', '忠县', '云阳县', '奉节县',
    '巫山县', '巫溪县', '石柱县', '秀山县', '酉阳县', '彭水县',
    # 自治县（简称）
    '石柱土家族自治县', '秀山土家族苗族自治县', 
    '酉阳土家族苗族自治县', '彭水苗族土家族自治县'
]


def update_district_batch(conn, city: str, district: str, batch_size: int = 500) -> int:
    """
    分批更新某个区县的数据
    每次只更新 batch_size 条记录，避免长时间锁定
    """
    full_district = f'{city}{district}'
    total_updated = 0
    
    while True:
        # 使用 LIMIT 分批更新
        result = conn.execute(text("""
            UPDATE evdata 
            SET 区县_中文 = :full_district
            WHERE 省份_中文 = :city 
            AND (区县_中文 = :city OR 区县_中文 IS NULL OR 区县_中文 = '')
            AND 充电站位置 LIKE :pattern
            LIMIT :batch_size
        """), {
            'full_district': full_district,
            'city': city,
            'pattern': f'%{district}%',
            'batch_size': batch_size
        })
        
        if result.rowcount == 0:
            break
        
        total_updated += result.rowcount
        conn.commit()  # 每批提交一次
        
        # 批次间短暂延迟，释放锁
        time.sleep(BATCH_DELAY)
    
    return total_updated


def main():
    print("=" * 60)
    print("  天津/重庆区县数据修复工具 v2")
    print("=" * 60)
    print(f"\n配置: 批量大小={BATCH_SIZE}, 锁超时={LOCK_TIMEOUT}秒")
    
    # 检查是否有--yes参数自动确认
    auto_confirm = '--yes' in sys.argv or '-y' in sys.argv
    
    engine = create_db_engine()
    
    # 统计修复前的数据
    print("\n📊 修复前统计:")
    with engine.connect() as conn:
        for city in ['天津市', '重庆市']:
            total = conn.execute(text(f"SELECT COUNT(*) FROM evdata WHERE 省份_中文 = '{city}'")).scalar()
            need_fix = conn.execute(text(f"""
                SELECT COUNT(*) FROM evdata 
                WHERE 省份_中文 = '{city}' 
                AND (区县_中文 = '{city}' OR 区县_中文 IS NULL OR 区县_中文 = '')
            """)).scalar()
            print(f"  {city}: 总记录 {total:,}，需修复 {need_fix:,}")
    
    # 确认是否继续
    if not auto_confirm:
        print("\n⚠️  此操作将修改数据库中的区县_中文字段")
        try:
            response = input("是否继续？(Y/N): ").strip().upper()
            if response != 'Y':
                print("已取消")
                engine.dispose()
                return
        except EOFError:
            print("\n提示: 使用 --yes 或 -y 参数自动确认执行")
            engine.dispose()
            return
    else:
        print("\n✅ 自动确认模式，开始执行...")
    
    # 开始修复
    print("\n🔧 开始修复（分批模式，每批 {} 条）...".format(BATCH_SIZE))
    
    fixed_count = {'天津市': 0, '重庆市': 0}
    failed_count = {'天津市': 0, '重庆市': 0}
    
    for city in ['天津市', '重庆市']:
        print(f"\n处理 {city}...")
        
        districts = TIANJIN_DISTRICTS if city == '天津市' else CHONGQING_DISTRICTS
        
        city_total = 0
        for district in districts:
            try:
                with engine.connect() as conn:
                    # 设置较长的锁等待超时
                    conn.execute(text(f"SET innodb_lock_wait_timeout = {LOCK_TIMEOUT}"))
                    
                    # 分批更新
                    count = update_district_batch(conn, city, district, BATCH_SIZE)
                    
                    if count > 0:
                        city_total += count
                        print(f"    {district}: 更新 {count:,} 条")
                    
            except Exception as e:
                error_msg = str(e)
                if 'Lock wait timeout' in error_msg:
                    print(f"    {district}: ⏳ 锁超时，等待后重试...")
                    time.sleep(5)  # 等待5秒后重试
                    try:
                        with engine.connect() as conn:
                            conn.execute(text(f"SET innodb_lock_wait_timeout = {LOCK_TIMEOUT}"))
                            count = update_district_batch(conn, city, district, BATCH_SIZE // 2)  # 减小批量重试
                            if count > 0:
                                city_total += count
                                print(f"    {district}: ✅ 重试成功，更新 {count:,} 条")
                    except Exception as e2:
                        print(f"    {district}: ❌ 重试失败 - {e2}")
                else:
                    print(f"    {district}: ❌ 失败 - {e}")
        
        fixed_count[city] = city_total
        print(f"  ✅ {city} 修复完成: {city_total:,} 条")
        
        # 统计无法修复的数量
        with engine.connect() as conn:
            remaining = conn.execute(text(f"""
                SELECT COUNT(*) FROM evdata 
                WHERE 省份_中文 = '{city}' 
                AND (区县_中文 = '{city}' OR 区县_中文 IS NULL OR 区县_中文 = '')
            """)).scalar()
            failed_count[city] = remaining
    
    # 修复后统计
    print("\n" + "=" * 60)
    print("📊 修复结果:")
    print("=" * 60)
    
    with engine.connect() as conn:
        for city in ['天津市', '重庆市']:
            # 获取修复后的区县分布
            districts = conn.execute(text(f"""
                SELECT 区县_中文, COUNT(*) as cnt 
                FROM evdata 
                WHERE 省份_中文 = '{city}' 
                GROUP BY 区县_中文 
                ORDER BY cnt DESC 
                LIMIT 10
            """)).fetchall()
            
            print(f"\n{city} 修复后区县分布 (Top 10):")
            for d in districts:
                print(f"  {d[0]}: {d[1]:,}")
            
            print(f"\n  成功修复: {fixed_count[city]:,} 条")
            print(f"  无法提取区县: {failed_count[city]:,} 条")
    
    engine.dispose()
    print("\n✅ 修复完成！")


if __name__ == '__main__':
    main()
