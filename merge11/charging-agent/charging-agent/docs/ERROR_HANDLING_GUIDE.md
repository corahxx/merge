# 错误处理指南

## 概述

系统已经集成了完善的错误处理机制，当遇到错误时会自动：

1. ✅ 记录详细的错误日志
2. ✅ 提供错误类型和消息
3. ✅ 给出针对性的解决方案
4. ✅ 保存错误堆栈跟踪

## 错误处理流程

### 1. 自动错误处理

所有模块都已经集成了错误处理器，错误会自动被捕获和处理：

```python
from data.data_processor import DataProcessor

processor = DataProcessor(verbose=True)

try:
    result = processor.process_excel_file('file.xlsx')
    print("✅ 处理成功")
except Exception as e:
    # 错误已经被自动处理和记录
    print("❌ 处理失败，请查看日志文件")
```

### 2. 查看错误日志

所有错误都会记录到 `data_processing.log` 文件中：

```bash
# 查看最新错误
tail -n 50 data_processing.log

# 搜索特定错误
grep "ERROR" data_processing.log
```

### 3. 使用诊断工具

运行系统诊断工具快速定位问题：

```bash
python test_data_system.py
```

这会检查：
- ✅ 数据库连接
- ✅ Python库安装
- ✅ 模块导入
- ✅ 配置完整性

### 4. 在Streamlit界面中

界面中的错误会自动显示：
- 简化的错误消息
- 可展开的详细错误信息
- 针对性的解决方案建议

点击错误信息下方的"查看详细错误信息"可以展开查看：
- 完整的错误堆栈
- 错误发生的时间
- 错误的上下文信息
- 可能的解决方案

## 手动使用错误处理器

如果需要手动处理错误：

```python
from data.error_handler import ErrorHandler

try:
    # 你的代码
    result = some_operation()
except Exception as e:
    # 处理错误
    error_info = ErrorHandler.handle_exception(e, "操作上下文")
    
    # 获取解决方案
    solutions = ErrorHandler.get_common_solutions(
        error_info['error_type'],
        error_info['error_message']
    )
    
    # 打印错误报告
    print(ErrorHandler.format_error_report(error_info))
    
    # 打印解决方案
    for solution in solutions:
        print(solution)
```

## 错误信息结构

错误信息字典包含以下字段：

```python
{
    'timestamp': '2024-12-17T20:30:00',  # 错误发生时间
    'context': '数据导入',                 # 错误上下文
    'error_type': 'ValueError',           # 错误类型
    'error_message': '详细错误消息',       # 错误消息
    'traceback': '完整堆栈跟踪...'        # 堆栈跟踪
}
```

## 常见错误类型及解决方案

### 数据库连接错误

**自动检测到时会提示：**
1. 检查数据库服务是否启动
2. 检查配置是否正确
3. 检查用户权限
4. 检查网络连接

### 文件读取错误

**自动检测到时会提示：**
1. 检查文件路径
2. 检查文件是否存在
3. 检查文件是否被占用
4. 检查文件格式

### 编码错误

**自动检测到时会提示：**
1. 检查文件编码
2. 转换为UTF-8
3. 处理特殊字符
4. 尝试不同编码

### SQL错误

**自动检测到时会提示：**
1. 检查SQL语法
2. 检查字段名
3. 检查数据类型
4. 检查特殊字符

更多错误类型和解决方案请参考 `TROUBLESHOOTING.md`。

## 最佳实践

### 1. 始终查看详细错误信息

不要只看简化的错误消息，展开查看完整的错误信息：

```python
# 在代码中
error_info = ErrorHandler.handle_exception(e, "上下文")
print(ErrorHandler.format_error_report(error_info))
```

### 2. 查看日志文件

定期检查日志文件，了解系统运行情况：

```bash
# 查看最近的错误
tail -n 100 data_processing.log | grep ERROR
```

### 3. 使用诊断工具

在遇到问题前，先运行诊断工具确保环境正常：

```bash
python test_data_system.py
```

### 4. 记录错误上下文

提供有意义的上下文信息有助于定位问题：

```python
try:
    processor.process_excel_file('file.xlsx')
except Exception as e:
    error_info = ErrorHandler.handle_exception(
        e, 
        f"处理文件 {file_path}，模式: {if_exists}"  # 详细的上下文
    )
```

## 日志级别

系统使用Python标准logging模块，支持以下级别：

- **INFO**: 正常操作信息
- **WARNING**: 警告信息（不影响运行）
- **ERROR**: 错误信息（操作失败）
- **DEBUG**: 调试信息（详细的技术细节）

默认级别是INFO，会记录INFO及以上级别的信息。

要查看DEBUG级别信息，可以修改日志配置：

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## 获取帮助

如果错误仍然无法解决：

1. **运行诊断工具**: `python test_data_system.py`
2. **查看日志文件**: `data_processing.log`
3. **查看故障排除指南**: `TROUBLESHOOTING.md`
4. **收集错误信息**: 完整的错误堆栈、配置信息、数据样本

