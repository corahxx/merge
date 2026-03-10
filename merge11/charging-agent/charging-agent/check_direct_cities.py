# -*- coding: utf-8 -*-
"""检查天津和重庆的充电站位置字段"""

import sys
sys.path.insert(0, '.')

from utils.db_utils import create_db_engine
from sqlalchemy import text

engine = create_db_engine()
with engine.connect() as conn:
    results = []
    
    for city in ['天津市', '重庆市']:
        # 获取样本数据：充电站位置字段
        samples = conn.execute(text(f"""
            SELECT DISTINCT 充电站位置, 充电站名称
            FROM evdata 
            WHERE 省份_中文 = '{city}' 
              AND 充电站位置 IS NOT NULL 
              AND 充电站位置 != ''
            LIMIT 20
        """)).fetchall()
        
        results.append({
            'city': city,
            'samples': samples
        })

# 写入文件
with open('direct_cities_result.txt', 'w', encoding='utf-8') as f:
    f.write('天津和重庆充电站位置字段检查:\n')
    f.write('=' * 60 + '\n\n')
    
    for r in results:
        f.write(f"【{r['city']}】样本数据:\n")
        f.write('-' * 60 + '\n')
        for i, sample in enumerate(r['samples'], 1):
            location = sample[0] or '(空)'
            name = sample[1] or '(空)'
            f.write(f"{i}. 位置: {location}\n")
            f.write(f"   名称: {name}\n\n")
        f.write('\n')

print('结果已写入 direct_cities_result.txt')
engine.dispose()
