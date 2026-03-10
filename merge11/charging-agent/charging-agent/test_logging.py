# test_logging.py - 测试日志功能

import sys
import os

# 将项目根目录加入路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.error_handler import ErrorHandler, logger

print("=" * 60)
print("测试日志功能")
print("=" * 60)

# 测试1: 记录INFO级别日志
print("\n1. 测试INFO级别日志...")
logger.info("这是一条INFO级别的测试日志")

# 测试2: 记录ERROR级别日志
print("2. 测试ERROR级别日志...")
logger.error("这是一条ERROR级别的测试日志")

# 测试3: 测试异常处理
print("3. 测试异常处理...")
try:
    raise ValueError("这是一个测试异常")
except Exception as e:
    ErrorHandler.handle_exception(e, "测试异常处理")

# 测试4: 检查日志文件
print("\n4. 检查日志文件...")
log_file = os.path.join(os.path.dirname(__file__), 'data_processing.log')
if os.path.exists(log_file):
    file_size = os.path.getsize(log_file)
    print(f"[OK] 日志文件存在: {log_file}")
    print(f"   文件大小: {file_size} 字节")
    
    # 读取最后几行
    with open(log_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        print(f"   总行数: {len(lines)}")
        print(f"\n   最后3行内容:")
        for line in lines[-3:]:
            print(f"   {line.strip()}")
else:
    print(f"[ERROR] 日志文件不存在: {log_file}")

# 测试5: 检查logger的handlers
print("\n5. 检查logger配置...")
root_logger = logger.root
print(f"   根logger handlers数量: {len(root_logger.handlers)}")
for i, handler in enumerate(root_logger.handlers):
    handler_type = type(handler).__name__
    if isinstance(handler, logger.handlers[0].__class__ if logger.handlers else type(None)):
        try:
            if hasattr(handler, 'baseFilename'):
                print(f"   Handler {i+1}: {handler_type} -> {handler.baseFilename}")
            else:
                print(f"   Handler {i+1}: {handler_type}")
        except:
            print(f"   Handler {i+1}: {handler_type} (无法获取详细信息)")

print("\n" + "=" * 60)
print("测试完成！")
print("=" * 60)

