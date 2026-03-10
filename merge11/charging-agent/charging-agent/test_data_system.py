# test_data_system.py - 数据系统测试和诊断工具

import sys
import os

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.error_handler import ErrorHandler, logger
from utils.db_utils import create_db_engine, test_connection, get_db_config_info


def test_database_connection():
    """测试数据库连接"""
    print("🔍 测试数据库连接...")
    try:
        # 显示配置信息
        config_info = get_db_config_info()
        print(f"   主机: {config_info['host']}")
        print(f"   端口: {config_info['port']}")
        print(f"   用户: {config_info['user']}")
        print(f"   数据库: {config_info['database']}")
        
        # 使用统一工具函数测试连接
        success, message = test_connection()
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        error_info = ErrorHandler.handle_exception(e, "数据库连接测试")
        print(ErrorHandler.format_error_report(error_info))
        return False


def test_table_exists():
    """测试表是否存在"""
    print("\n🔍 测试数据表是否存在...")
    try:
        from sqlalchemy import inspect
        
        engine = create_db_engine(echo=False)  # 使用统一工具函数
        inspector = inspect(engine)
        
        table_name = 'table2509ev'
        if table_name in inspector.get_table_names():
            print(f"✅ 表 '{table_name}' 存在")
            
            # 显示表结构
            columns = inspector.get_columns(table_name)
            print(f"\n📊 表结构 ({len(columns)} 个字段):")
            for col in columns:
                print(f"  - {col['name']}: {col['type']}")
            return True
        else:
            print(f"⚠️  表 '{table_name}' 不存在")
            print("💡 提示：可以使用 'replace' 模式导入数据来自动创建表")
            return False
    except Exception as e:
        error_info = ErrorHandler.handle_exception(e, "表存在性检查")
        print(ErrorHandler.format_error_report(error_info))
        return False


def test_excel_libraries():
    """测试EXCEL相关库是否安装"""
    print("\n🔍 测试EXCEL处理库...")
    libraries = {
        'pandas': 'pandas',
        'openpyxl': 'openpyxl',
        'xlrd': 'xlrd',
        'sqlalchemy': 'sqlalchemy',
        'pymysql': 'pymysql'
    }
    
    all_ok = True
    for lib_name, import_name in libraries.items():
        try:
            __import__(import_name)
            print(f"✅ {lib_name} 已安装")
        except ImportError:
            print(f"❌ {lib_name} 未安装")
            print(f"   请运行: pip install {lib_name}")
            all_ok = False
    
    return all_ok


def test_data_modules():
    """测试数据模块是否正常导入"""
    print("\n🔍 测试数据模块...")
    modules = [
        'data.excel_reader',
        'data.data_cleaner',
        'data.data_loader',
        'data.data_analyzer',
        'data.data_processor'
    ]
    
    all_ok = True
    for module_name in modules:
        try:
            __import__(module_name)
            print(f"✅ {module_name} 导入成功")
        except Exception as e:
            print(f"❌ {module_name} 导入失败: {str(e)}")
            all_ok = False
    
    return all_ok


def test_knowledge_base():
    """测试知识库"""
    print("\n🔍 测试知识库...")
    try:
        from core.knowledge_base import KnowledgeBase
        
        # 测试运营商映射
        test_operator = KnowledgeBase.normalize_operator("星星")
        print(f"✅ 运营商映射测试: '星星' -> '{test_operator}'")
        
        # 测试地理位置映射
        test_location = KnowledgeBase.normalize_location("浦东")
        print(f"✅ 地理位置映射测试: '浦东' -> '{test_location}'")
        
        return True
    except Exception as e:
        error_info = ErrorHandler.handle_exception(e, "知识库测试")
        print(ErrorHandler.format_error_report(error_info))
        return False


def check_config():
    """检查配置"""
    print("\n🔍 检查配置...")
    try:
        # 使用工具函数获取配置信息
        config_info = get_db_config_info()
        print(f"数据库主机: {config_info['host']}")
        print(f"数据库端口: {config_info['port']}")
        print(f"数据库名称: {config_info['database']}")
        print(f"数据库用户: {config_info['user']}")
        print(f"密码: {'已设置' if config_info['password_set'] else '未设置'}")
        
        # 检查必要的配置项
        required_keys = ['host', 'port', 'user', 'database']
        missing_keys = [key for key in required_keys if key not in config_info or not config_info[key]]
        
        if missing_keys:
            print(f"❌ 缺少配置项: {', '.join(missing_keys)}")
            return False
        else:
            print("✅ 配置完整")
            return True
    except Exception as e:
        error_info = ErrorHandler.handle_exception(e, "配置检查")
        print(ErrorHandler.format_error_report(error_info))
        return False


def main():
    """运行所有测试"""
    print("="*60)
    print("🔧 数据系统诊断工具")
    print("="*60)
    
    results = []
    
    # 运行各项测试
    results.append(("配置检查", check_config()))
    results.append(("EXCEL库检查", test_excel_libraries()))
    results.append(("数据模块检查", test_data_modules()))
    results.append(("知识库检查", test_knowledge_base()))
    results.append(("数据库连接", test_database_connection()))
    results.append(("数据表检查", test_table_exists()))
    
    # 汇总结果
    print("\n" + "="*60)
    print("📊 测试结果汇总")
    print("="*60)
    
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{name}: {status}")
    
    all_passed = all(result for _, result in results)
    
    print("="*60)
    if all_passed:
        print("🎉 所有测试通过！系统可以正常使用。")
    else:
        print("⚠️  部分测试失败，请根据上面的提示修复问题。")
        print("\n💡 常见问题解决方案:")
        print("1. 安装依赖: pip install -r requirements.txt")
        print("2. 检查数据库配置: 编辑 config.py")
        print("3. 检查数据库服务是否启动")
        print("4. 查看日志文件: data_processing.log")
    print("="*60)


if __name__ == "__main__":
    main()

