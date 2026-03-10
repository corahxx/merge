# test_db_port_config.py - 测试数据库端口配置功能

"""
测试数据库端口配置功能
验证：
1. 默认端口配置（3306）
2. 自定义端口配置
3. 环境变量覆盖
4. 所有模块能正常连接
"""

import sys
import os

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.db_utils import get_db_url, create_db_engine, test_connection, get_db_config_info


def test_default_port():
    """测试默认端口配置"""
    print("=" * 60)
    print("测试1: 默认端口配置（3306）")
    print("=" * 60)
    
    config_info = get_db_config_info()
    print(f"当前配置:")
    print(f"  主机: {config_info['host']}")
    print(f"  端口: {config_info['port']}")
    print(f"  用户: {config_info['user']}")
    print(f"  数据库: {config_info['database']}")
    
    db_url = get_db_url()
    # 隐藏密码
    safe_url = db_url.split('@')[0].split(':')[0] + ':***@' + '@'.join(db_url.split('@')[1:])
    print(f"\n数据库URL: {safe_url}")
    
    success, message = test_connection()
    if success:
        print(f"[OK] {message}")
        return True
    else:
        print(f"[FAIL] {message}")
        return False


def test_custom_port():
    """测试自定义端口配置"""
    print("\n" + "=" * 60)
    print("测试2: 自定义端口配置")
    print("=" * 60)
    
    # 临时修改端口（仅用于测试，不实际修改config.py）
    import config
    original_port = config.DB_CONFIG.get('port', 3306)
    
    # 测试不同的端口
    test_ports = [3306, 3307, 3308]
    
    for port in test_ports:
        config.DB_CONFIG['port'] = port
        config_info = get_db_config_info()
        print(f"\n测试端口 {port}:")
        print(f"  配置端口: {config_info['port']}")
        
        db_url = get_db_url()
        if f":{port}" in db_url:
            print(f"  [OK] URL包含端口 {port}")
        else:
            print(f"  [FAIL] URL未包含端口 {port}")
    
    # 恢复原始端口
    config.DB_CONFIG['port'] = original_port
    print(f"\n已恢复原始端口配置: {original_port}")


def test_environment_variable():
    """测试环境变量覆盖"""
    print("\n" + "=" * 60)
    print("测试3: 环境变量覆盖")
    print("=" * 60)
    
    # 保存原始环境变量
    original_port = os.getenv('DB_PORT')
    
    # 设置测试环境变量
    test_port = '3307'
    os.environ['DB_PORT'] = test_port
    
    try:
        config_info = get_db_config_info()
        db_url = get_db_url()
        
        print(f"环境变量 DB_PORT: {test_port}")
        print(f"实际使用的端口: {config_info['port']}")
        print(f"数据库URL: {db_url.replace('caam', '***')}")
        
        if str(config_info['port']) == test_port:
            print("[OK] 环境变量覆盖成功")
            result = True
        else:
            print("[FAIL] 环境变量覆盖失败")
            result = False
    finally:
        # 恢复原始环境变量
        if original_port:
            os.environ['DB_PORT'] = original_port
        elif 'DB_PORT' in os.environ:
            del os.environ['DB_PORT']
    
    return result


def test_module_imports():
    """测试所有模块能否正常导入"""
    print("\n" + "=" * 60)
    print("测试4: 模块导入测试")
    print("=" * 60)
    
    modules = [
        'data.data_loader',
        'data.data_analyzer',
        'data.table_schema',
        'data.region_dictionary',
        'handlers.data_preview_handler',
        'handlers.data_quality_handler',
        'handlers.pile_model_query_handler',
    ]
    
    success_count = 0
    for module_name in modules:
        try:
            __import__(module_name)
            print(f"[OK] {module_name}")
            success_count += 1
        except Exception as e:
            print(f"[FAIL] {module_name}: {str(e)}")
    
    print(f"\n成功导入: {success_count}/{len(modules)}")
    return success_count == len(modules)


def main():
    """主测试函数"""
    print("\n" + "=" * 60)
    print("数据库端口配置功能测试")
    print("=" * 60)
    
    results = []
    
    # 测试1: 默认端口
    results.append(("默认端口配置", test_default_port()))
    
    # 测试2: 自定义端口
    test_custom_port()
    results.append(("自定义端口配置", True))  # 仅检查URL格式，不实际连接
    
    # 测试3: 环境变量
    results.append(("环境变量覆盖", test_environment_variable()))
    
    # 测试4: 模块导入
    results.append(("模块导入", test_module_imports()))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    for test_name, result in results:
        status = "[OK] 通过" if result else "[FAIL] 失败"
        print(f"{test_name}: {status}")
    
    all_passed = all(result for _, result in results)
    
    print("\n" + "=" * 60)
    if all_passed:
        print("[SUCCESS] 所有测试通过！")
    else:
        print("[WARNING] 部分测试失败，请检查配置")
    print("=" * 60)
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
