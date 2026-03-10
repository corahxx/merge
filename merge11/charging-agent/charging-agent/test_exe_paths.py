#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试EXE路径检测
用于诊断打包后的路径问题
"""

import sys
import os
from pathlib import Path

print("=" * 60)
print("EXE路径检测测试")
print("=" * 60)
print()

# 检测运行模式
is_frozen = getattr(sys, 'frozen', False)
print(f"运行模式: {'EXE (打包后)' if is_frozen else 'Python脚本 (开发环境)'}")
print()

# 显示关键路径
print("关键路径信息:")
print("-" * 60)

if is_frozen:
    print(f"sys.executable: {sys.executable}")
    print(f"EXE所在目录: {Path(sys.executable).parent}")
    print(f"sys._MEIPASS: {getattr(sys, '_MEIPASS', 'N/A')}")
else:
    print(f"__file__: {__file__}")
    print(f"脚本所在目录: {Path(__file__).parent}")

print(f"当前工作目录: {os.getcwd()}")
print(f"sys.path[0]: {sys.path[0]}")
print()

# 测试路径检测逻辑
print("路径检测测试:")
print("-" * 60)

if is_frozen:
    BASE_DIR = Path(sys.executable).parent.resolve()
else:
    BASE_DIR = Path(__file__).parent.resolve()

print(f"基础目录: {BASE_DIR}")
print(f"基础目录是否存在: {BASE_DIR.exists()}")

APP_DIR = BASE_DIR / "app"
print(f"app目录: {APP_DIR}")
print(f"app目录是否存在: {APP_DIR.exists()}")

if not APP_DIR.exists():
    APP_DIR = BASE_DIR
    print(f"使用基础目录: {APP_DIR}")

print()
print("目录内容:")
print("-" * 60)
try:
    if APP_DIR.exists():
        files = list(APP_DIR.glob("*.py"))[:10]
        print(f"Python文件 ({len(files)} 个):")
        for f in files:
            print(f"  - {f.name}")
    else:
        print("目录不存在")
except Exception as e:
    print(f"错误: {str(e)}")

print()
print("查找关键文件:")
print("-" * 60)
for filename in ["main.py", "data_manager.py", "config.py"]:
    filepath = APP_DIR / filename
    exists = filepath.exists()
    status = "✅" if exists else "❌"
    print(f"{status} {filename}: {filepath}")

print()
print("=" * 60)
