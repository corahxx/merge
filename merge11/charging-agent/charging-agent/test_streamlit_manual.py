#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
手动测试Streamlit启动
用于诊断问题
"""

import sys
import os
from pathlib import Path

# 设置路径
APP_DIR = Path(__file__).parent
sys.path.insert(0, str(APP_DIR))

print("=" * 60)
print("Streamlit手动测试")
print("=" * 60)
print()

# 检查文件
main_script = APP_DIR / "main.py"
if not main_script.exists():
    main_script = APP_DIR / "data_manager.py"

if not main_script.exists():
    print("❌ 未找到main.py或data_manager.py")
    sys.exit(1)

print(f"📁 应用目录: {APP_DIR}")
print(f"📄 启动文件: {main_script}")
print()

# 检查配置
config_file = APP_DIR / "config.py"
if not config_file.exists():
    print("⚠️  配置文件不存在")
else:
    print("✅ 配置文件存在")
    try:
        from config import DB_CONFIG
        print(f"   数据库: {DB_CONFIG.get('host')}:{DB_CONFIG.get('port')}/{DB_CONFIG.get('database')}")
    except Exception as e:
        print(f"   ⚠️  配置加载失败: {str(e)}")

print()

# 检查依赖
print("🔍 检查依赖...")
try:
    import streamlit
    print(f"✅ streamlit: {streamlit.__version__}")
except ImportError as e:
    print(f"❌ streamlit未安装: {str(e)}")
    sys.exit(1)

try:
    import pandas
    print(f"✅ pandas: {pandas.__version__}")
except ImportError as e:
    print(f"❌ pandas未安装: {str(e)}")

try:
    import sqlalchemy
    print(f"✅ sqlalchemy: {sqlalchemy.__version__}")
except ImportError as e:
    print(f"❌ sqlalchemy未安装: {str(e)}")

print()

# 测试数据库连接
print("🔍 测试数据库连接...")
try:
    from utils.db_utils import test_connection
    success, message = test_connection()
    if success:
        print(f"✅ {message}")
    else:
        print(f"❌ {message}")
except Exception as e:
    print(f"❌ 连接测试失败: {str(e)}")
    import traceback
    traceback.print_exc()

print()

# 启动Streamlit
print("🚀 启动Streamlit...")
print("   命令: streamlit run", main_script.name)
print("   按 Ctrl+C 停止")
print("=" * 60)
print()

import subprocess
cmd = [
    sys.executable,
    "-m", "streamlit", "run",
    str(main_script),
    "--server.maxUploadSize=600",
    "--server.maxMessageSize=600",
]

try:
    subprocess.run(cmd, cwd=str(APP_DIR))
except KeyboardInterrupt:
    print("\n✅ 已停止")
