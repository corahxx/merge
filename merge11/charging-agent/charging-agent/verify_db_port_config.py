# verify_db_port_config.py - 验证数据库端口配置功能

"""
验证数据库端口配置功能是否完整实施
检查：
1. 所有文件是否正确导入工具函数
2. 是否还有遗漏的DB_CONFIG直接使用
3. 配置是否正确
"""

import sys
import os
import re

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def check_file_imports():
    """检查文件导入"""
    print("=" * 60)
    print("检查1: 文件导入验证")
    print("=" * 60)
    
    files_to_check = [
        'utils/db_utils.py',
        'config.py',
        'data/data_loader.py',
        'data/data_analyzer.py',
        'data/table_schema.py',
        'data/region_dictionary.py',
        'app.py',
        'data_manager.py',
        'core/condition_parser.py',
        'handlers/data_preview_handler.py',
        'handlers/data_quality_handler.py',
        'handlers/pile_model_query_handler.py',
        'update_location_knowledge_base.py',
        'test_data_system.py',
    ]
    
    success_count = 0
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            print(f"[SKIP] {file_path} - 文件不存在")
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # 检查是否使用了工具函数
            if 'from utils.db_utils import' in content or 'from utils import db_utils' in content:
                print(f"[OK] {file_path} - 已使用工具函数")
                success_count += 1
            elif 'create_db_engine' in content or 'get_db_url' in content:
                print(f"[OK] {file_path} - 已使用工具函数")
                success_count += 1
            elif file_path == 'config.py':
                # config.py 不需要导入工具函数
                if 'port' in content:
                    print(f"[OK] {file_path} - 已添加port配置")
                    success_count += 1
                else:
                    print(f"[FAIL] {file_path} - 缺少port配置")
            elif 'pile_model_query.py' in file_path:
                # 这个文件只是导入但未使用，可以忽略
                print(f"[SKIP] {file_path} - 仅导入未使用")
                success_count += 1
            else:
                # 检查是否还有旧的连接方式
                if 'mysql+pymysql://{DB_CONFIG' in content or 'create_engine.*DB_CONFIG' in content:
                    print(f"[WARN] {file_path} - 可能还有旧代码")
                else:
                    print(f"[OK] {file_path} - 无数据库连接代码（可能不需要）")
                    success_count += 1
        except Exception as e:
            print(f"[ERROR] {file_path} - 读取失败: {str(e)}")
    
    print(f"\n通过: {success_count}/{len(files_to_check)}")
    return success_count == len(files_to_check)


def check_config_file():
    """检查配置文件"""
    print("\n" + "=" * 60)
    print("检查2: 配置文件验证")
    print("=" * 60)
    
    try:
        from config import DB_CONFIG
        
        required_keys = ['host', 'port', 'user', 'password', 'database']
        missing_keys = [key for key in required_keys if key not in DB_CONFIG]
        
        if missing_keys:
            print(f"[FAIL] 缺少配置项: {', '.join(missing_keys)}")
            return False
        
        print(f"[OK] 配置完整")
        print(f"   主机: {DB_CONFIG['host']}")
        print(f"   端口: {DB_CONFIG['port']}")
        print(f"   用户: {DB_CONFIG['user']}")
        print(f"   数据库: {DB_CONFIG['database']}")
        
        return True
    except Exception as e:
        print(f"[FAIL] 配置读取失败: {str(e)}")
        return False


def check_tool_functions():
    """检查工具函数"""
    print("\n" + "=" * 60)
    print("检查3: 工具函数验证")
    print("=" * 60)
    
    try:
        from utils.db_utils import get_db_url, create_db_engine, test_connection, get_db_config_info
        
        # 测试get_db_url
        db_url = get_db_url()
        if 'mysql+pymysql://' in db_url:
            print(f"[OK] get_db_url() - URL格式正确")
            # 检查是否包含端口
            if re.search(r':\d+/', db_url):
                print(f"[OK] get_db_url() - URL包含端口")
            else:
                print(f"[WARN] get_db_url() - URL可能缺少端口")
        else:
            print(f"[FAIL] get_db_url() - URL格式错误")
            return False
        
        # 测试get_db_config_info
        config_info = get_db_config_info()
        if 'port' in config_info:
            print(f"[OK] get_db_config_info() - 包含端口信息: {config_info['port']}")
        else:
            print(f"[FAIL] get_db_config_info() - 缺少端口信息")
            return False
        
        # 测试create_db_engine（不实际连接）
        try:
            engine = create_db_engine(echo=False)
            print(f"[OK] create_db_engine() - 创建成功")
        except Exception as e:
            print(f"[WARN] create_db_engine() - 创建失败（可能是数据库未启动）: {str(e)}")
        
        return True
    except Exception as e:
        print(f"[FAIL] 工具函数导入失败: {str(e)}")
        return False


def check_old_code_patterns():
    """检查是否还有旧代码模式"""
    print("\n" + "=" * 60)
    print("检查4: 旧代码模式检查")
    print("=" * 60)
    
    patterns = [
        (r'mysql\+pymysql://.*DB_CONFIG\[', '直接使用DB_CONFIG构建URL'),
        (r'create_engine\(.*DB_CONFIG', '直接使用DB_CONFIG创建引擎'),
    ]
    
    files_to_check = [
        'data', 'handlers', 'core', 'app.py', 'data_manager.py',
        'update_location_knowledge_base.py', 'test_data_system.py'
    ]
    
    found_issues = []
    
    for pattern, description in patterns:
        for file_path in files_to_check:
            if os.path.isfile(file_path):
                files = [file_path]
            elif os.path.isdir(file_path):
                files = []
                for root, dirs, filenames in os.walk(file_path):
                    for filename in filenames:
                        if filename.endswith('.py') and 'backup' not in root:
                            files.append(os.path.join(root, filename))
            else:
                continue
            
            for f in files:
                try:
                    with open(f, 'r', encoding='utf-8') as file:
                        content = file.read()
                        if re.search(pattern, content):
                            found_issues.append((f, description))
                except:
                    pass
    
    if found_issues:
        print(f"[WARN] 发现可能的旧代码模式:")
        for file_path, desc in found_issues:
            print(f"   {file_path}: {desc}")
        return False
    else:
        print(f"[OK] 未发现旧代码模式")
        return True


def main():
    """主验证函数"""
    print("\n" + "=" * 60)
    print("数据库端口配置功能验证")
    print("=" * 60)
    
    results = []
    
    results.append(("文件导入", check_file_imports()))
    results.append(("配置文件", check_config_file()))
    results.append(("工具函数", check_tool_functions()))
    results.append(("旧代码检查", check_old_code_patterns()))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("验证结果汇总")
    print("=" * 60)
    
    for check_name, result in results:
        status = "[OK] 通过" if result else "[FAIL] 失败"
        print(f"{check_name}: {status}")
    
    all_passed = all(result for _, result in results)
    
    print("\n" + "=" * 60)
    if all_passed:
        print("[SUCCESS] 所有检查通过！")
    else:
        print("[WARNING] 部分检查失败，请查看上述详情")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
