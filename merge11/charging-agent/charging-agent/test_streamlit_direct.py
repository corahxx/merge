# -*- coding: utf-8 -*-
"""
直接测试Streamlit启动
用于诊断问题
"""
import sys
import os
from pathlib import Path

# 设置工作目录
BASE_DIR = Path(__file__).parent
APP_DIR = BASE_DIR / "app"

os.chdir(str(APP_DIR))
sys.path.insert(0, str(APP_DIR))

print("=" * 60)
print("  直接测试Streamlit启动")
print("=" * 60)
print(f"工作目录: {os.getcwd()}")
print(f"Python路径: {sys.path[0]}")
print(f"Python版本: {sys.version}")
print()

# 检查main.py
main_script = APP_DIR / "main.py"
if not main_script.exists():
    print(f"❌ 错误: 未找到 main.py")
    print(f"   查找位置: {main_script}")
    sys.exit(1)

print(f"✅ 找到脚本: {main_script}")
print()

# 尝试导入streamlit
print("正在导入streamlit...")
try:
    import streamlit.web.cli as stcli
    print("✅ streamlit导入成功")
except Exception as e:
    print(f"❌ streamlit导入失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()
print("=" * 60)
print("  启动Streamlit...")
print("=" * 60)
print()

# 设置启动参数
sys.argv = [
    'streamlit',
    'run',
    str(main_script),
    '--server.maxUploadSize=600',
    '--server.maxMessageSize=600',
    '--server.headless=true',
]

print(f"启动参数: {sys.argv}")
print()
print("注意: Streamlit的输出将直接显示在这里")
print("按 Ctrl+C 停止")
print()
print("=" * 60)
print()

# 启动Streamlit
try:
    stcli.main()
except KeyboardInterrupt:
    print("\n\n已停止")
except Exception as e:
    print(f"\n\n❌ 启动失败: {e}")
    import traceback
    traceback.print_exc()
