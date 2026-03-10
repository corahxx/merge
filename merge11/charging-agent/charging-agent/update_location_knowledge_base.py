# -*- coding: utf-8 -*-
# update_location_knowledge_base.py - 从数据库更新区域知识库

"""
功能：从数据库中查询省份_中文、城市_中文、区县_中文三个字段的全部信息，并更新到知识库

使用方法：
    python update_location_knowledge_base.py [table_name]
    
    如果不提供表名，默认使用 'evdata'
"""

import sys
import os
from sqlalchemy import text
from utils.db_utils import create_db_engine

# 默认表名
DEFAULT_TABLE_NAME = 'evdata'


def get_database_regions(table_name: str = DEFAULT_TABLE_NAME):
    """
    从数据库查询所有区域信息
    :param table_name: 数据表名
    :return: (provinces, cities, districts) 元组，每个都是去重后的列表
    """
    print(f"🔍 正在连接数据库并查询表 '{table_name}' 的区域信息...")
    
    # 创建数据库连接（使用统一工具函数）
    engine = create_db_engine(echo=False)
    
    try:
        with engine.connect() as conn:
            # 查询省份
            print("📊 查询省份信息...")
            province_query = text(f"""
                SELECT DISTINCT `省份_中文`
                FROM `{table_name}`
                WHERE `省份_中文` IS NOT NULL 
                  AND `省份_中文` != ''
                ORDER BY `省份_中文`
            """)
            provinces = [row[0] for row in conn.execute(province_query).fetchall()]
            print(f"   找到 {len(provinces)} 个省份")
            
            # 查询城市
            print("📊 查询城市信息...")
            city_query = text(f"""
                SELECT DISTINCT `城市_中文`
                FROM `{table_name}`
                WHERE `城市_中文` IS NOT NULL 
                  AND `城市_中文` != ''
                ORDER BY `城市_中文`
            """)
            cities = [row[0] for row in conn.execute(city_query).fetchall()]
            print(f"   找到 {len(cities)} 个城市")
            
            # 查询区县
            print("📊 查询区县信息...")
            district_query = text(f"""
                SELECT DISTINCT `区县_中文`
                FROM `{table_name}`
                WHERE `区县_中文` IS NOT NULL 
                  AND `区县_中文` != ''
                ORDER BY `区县_中文`
            """)
            districts = [row[0] for row in conn.execute(district_query).fetchall()]
            print(f"   找到 {len(districts)} 个区县")
            
            return provinces, cities, districts
            
    except Exception as e:
        print(f"❌ 查询数据库失败: {str(e)}")
        raise


def generate_location_mappings(provinces, cities, districts):
    """
    生成区域映射字典
    :param provinces: 省份列表
    :param cities: 城市列表
    :param districts: 区县列表
    :return: 映射字典
    """
    print("\n📝 正在生成区域映射...")
    
    location_mappings = {}
    
    # 处理省份
    print("   处理省份映射...")
    for province in provinces:
        if not province or province.strip() == '':
            continue
        
        # 省份全名（如"河南省"）
        province_full = province.strip()
        
        # 生成简称映射（去掉"省"字）
        if province_full.endswith('省'):
            province_short = province_full[:-1]  # "河南"
            if province_short and province_short not in location_mappings:
                location_mappings[province_short] = province_full
        
        # 直辖市特殊处理
        if province_full in ['北京市', '上海市', '天津市', '重庆市']:
            city_short = province_full[:-1]  # "北京"
            if city_short and city_short not in location_mappings:
                location_mappings[city_short] = province_full
    
    # 处理城市
    print("   处理城市映射...")
    for city in cities:
        if not city or city.strip() == '':
            continue
        
        city_full = city.strip()
        
        # 生成简称映射（去掉"市"字）
        if city_full.endswith('市'):
            city_short = city_full[:-1]  # "郑州"
            if city_short and city_short not in location_mappings:
                location_mappings[city_short] = city_full
    
    # 处理区县
    print("   处理区县映射...")
    for district in districts:
        if not district or district.strip() == '':
            continue
        
        district_full = district.strip()
        
        # 提取区县名称（去掉前面的省市信息）
        # 例如："北京市朝阳区" -> "朝阳"
        if '区' in district_full:
            # 找到最后一个"区"的位置
            district_name = district_full.split('区')[0]
            # 去掉省市前缀
            if '市' in district_name:
                district_name = district_name.split('市')[-1]
            elif '省' in district_name:
                district_name = district_name.split('省')[-1]
            
            if district_name and district_name not in location_mappings:
                location_mappings[district_name] = district_full
        
        elif '县' in district_full:
            # 处理县
            district_name = district_full.split('县')[0]
            if '市' in district_name:
                district_name = district_name.split('市')[-1]
            elif '省' in district_name:
                district_name = district_name.split('省')[-1]
            
            if district_name and district_name not in location_mappings:
                location_mappings[district_name] = district_full
    
    print(f"   生成了 {len(location_mappings)} 个映射关系")
    return location_mappings


def format_knowledge_base_code(location_mappings):
    """
    格式化知识库代码
    :param location_mappings: 映射字典 {简称: 全称}
    :return: 格式化的代码字符串
    """
    print("\n📝 正在生成知识库代码...")
    
    code_lines = []
    code_lines.append("    # 区域标准化映射（用户可能说的简称 → 标准格式）")
    code_lines.append("    # 自动生成，请勿手动修改")
    code_lines.append("    LOCATION_NICKNAMES = {")
    
    # 按全称排序，以便按省份分组显示
    sorted_items = sorted(location_mappings.items(), key=lambda x: x[1])
    
    current_province = None
    for short_name, full_name in sorted_items:
        # 提取省份（用于分组显示）
        province = None
        if '省' in full_name:
            province = full_name.split('省')[0] + '省'
        elif full_name in ['北京市', '上海市', '天津市', '重庆市']:
            province = full_name
        
        # 如果是新省份，添加注释
        if province and province != current_province:
            if current_province is not None:
                code_lines.append("")  # 省份之间空行
            code_lines.append(f"        # {province}")
            current_province = province
        
        # 添加映射
        code_lines.append(f'        "{short_name}": "{full_name}",')
    
    code_lines.append("    }")
    
    return "\n".join(code_lines)


def update_knowledge_base_file(new_code: str, backup: bool = True):
    """
    更新知识库文件
    :param new_code: 新的代码内容
    :param backup: 是否备份原文件
    """
    knowledge_base_file = "core/knowledge_base.py"
    
    print(f"\n📝 正在更新知识库文件: {knowledge_base_file}")
    
    # 读取原文件
    with open(knowledge_base_file, 'r', encoding='utf-8') as f:
        original_lines = f.readlines()
    
    # 备份原文件
    if backup:
        backup_file = f"{knowledge_base_file}.backup"
        with open(backup_file, 'w', encoding='utf-8') as f:
            f.writelines(original_lines)
        print(f"   ✅ 已备份原文件到: {backup_file}")
    
    # 找到 LOCATION_NICKNAMES 的起始和结束位置
    start_idx = None
    end_idx = None
    
    for i, line in enumerate(original_lines):
        if 'LOCATION_NICKNAMES = {' in line:
            # 找到注释行（向前查找）
            for j in range(i, max(0, i-5), -1):
                if '# 区域标准化映射' in original_lines[j]:
                    start_idx = j
                    break
            if start_idx is None:
                start_idx = i
            break
    
    if start_idx is not None:
        # 找到结束位置（匹配的右大括号）
        brace_count = 0
        found_start = False
        for i in range(start_idx, len(original_lines)):
            line = original_lines[i]
            if 'LOCATION_NICKNAMES = {' in line:
                found_start = True
                brace_count = line.count('{') - line.count('}')
            elif found_start:
                brace_count += line.count('{') - line.count('}')
                if brace_count == 0:
                    end_idx = i
                    break
        
        if end_idx is not None:
            # 替换
            new_lines = (
                original_lines[:start_idx] + 
                [line + '\n' if not line.endswith('\n') else line for line in new_code.split('\n')] +
                ['\n'] +  # 确保有换行
                original_lines[end_idx + 1:]
            )
            
            # 写入文件
            with open(knowledge_base_file, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            
            print(f"   ✅ 已更新知识库文件（替换了第 {start_idx+1} 到 {end_idx+1} 行）")
        else:
            print(f"   ⚠️  未找到 LOCATION_NICKNAMES 字典的结束位置")
            print(f"   💾 新代码已保存到: location_knowledge_base_new.py")
            with open('location_knowledge_base_new.py', 'w', encoding='utf-8') as f:
                f.write(new_code)
    else:
        print(f"   ⚠️  未找到 LOCATION_NICKNAMES 字典，尝试追加...")
        # 如果找不到，尝试在 OPERATOR_NICKNAMES 之后插入
        operator_end = None
        for i, line in enumerate(original_lines):
            if 'OPERATOR_NICKNAMES = {' in line:
                brace_count = line.count('{') - line.count('}')
                for j in range(i+1, len(original_lines)):
                    brace_count += original_lines[j].count('{') - original_lines[j].count('}')
                    if brace_count == 0:
                        operator_end = j
                        break
                break
        
        if operator_end is not None:
            new_lines = (
                original_lines[:operator_end + 2] +  # 包含结束大括号和空行
                ['\n'] +
                [line + '\n' if not line.endswith('\n') else line for line in new_code.split('\n')] +
                ['\n'] +
                original_lines[operator_end + 2:]
            )
            with open(knowledge_base_file, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            print(f"   ✅ 已追加到知识库文件")
        else:
            print(f"   ❌ 无法找到插入位置，请手动更新")
            print(f"   💾 新代码已保存到: location_knowledge_base_new.py")
            with open('location_knowledge_base_new.py', 'w', encoding='utf-8') as f:
                f.write(new_code)


def main():
    """主函数"""
    # 获取表名参数
    table_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TABLE_NAME
    
    print("=" * 60)
    print("🚀 区域知识库更新工具")
    print("=" * 60)
    print(f"📋 目标表: {table_name}")
    print()
    
    try:
        # 1. 查询数据库
        provinces, cities, districts = get_database_regions(table_name)
        
        if not provinces and not cities and not districts:
            print("❌ 未找到任何区域信息，请检查表名和数据库连接")
            return
        
        print(f"\n✅ 查询完成:")
        print(f"   省份: {len(provinces)} 个")
        print(f"   城市: {len(cities)} 个")
        print(f"   区县: {len(districts)} 个")
        
        # 2. 生成映射
        location_mappings = generate_location_mappings(provinces, cities, districts)
        
        # 3. 生成代码
        new_code = format_knowledge_base_code(location_mappings)
        
        # 4. 显示预览
        print("\n" + "=" * 60)
        print("📋 生成的代码预览（前50行）:")
        print("=" * 60)
        code_lines = new_code.split('\n')
        preview_lines = code_lines[:50]
        for line in preview_lines:
            print(line)
        total_lines = len(code_lines)
        if total_lines > 50:
            print(f"... (共 {total_lines} 行)")
        
        # 5. 确认更新
        print("\n" + "=" * 60)
        confirm = input("是否更新知识库文件? (y/n): ").strip().lower()
        if confirm == 'y':
            update_knowledge_base_file(new_code)
            print("\n✅ 知识库更新完成！")
        else:
            print("\n❌ 已取消更新")
            # 保存到临时文件
            temp_file = "location_knowledge_base_temp.py"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(new_code)
            print(f"💾 代码已保存到: {temp_file}")
        
    except Exception as e:
        print(f"\n❌ 发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

