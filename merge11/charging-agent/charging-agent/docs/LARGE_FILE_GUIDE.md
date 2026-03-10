# 大文件处理指南

## 概述

系统已针对大文件（600M+）进行了优化，支持：
- ✅ 分块读取和处理
- ✅ 内存优化
- ✅ 进度显示
- ✅ 自动选择处理模式

## 文件大小建议

| 文件大小 | 处理方式 | 建议 |
|---------|---------|------|
| < 50MB | 普通模式 | 一次性读取处理 |
| 50-200MB | 普通模式 | 可以使用普通模式，但建议使用大文件模式 |
| 200MB-1GB | 大文件模式 | 自动启用分块处理 |
| > 1GB | 大文件模式 | 强烈建议转换为CSV格式 |

## 使用方法

### 1. 通过Streamlit界面

界面会自动检测文件大小：
- **大于100MB的文件**：自动启用大文件处理模式
- **显示进度条**：实时显示处理进度
- **可配置批次大小**：根据内存情况调整

### 2. 通过代码使用

```python
from data.data_processor_large import DataProcessorLarge

# 初始化大文件处理器
processor = DataProcessorLarge(
    table_name='table2509ev',
    chunk_size=5000,  # 每批处理5000行
    verbose=True
)

# 处理大文件
result = processor.process_large_file(
    file_path='large_file.xlsx',
    field_mapping={...},
    if_exists='append'
)
```

### 3. 自定义进度回调

```python
def my_progress_callback(stage, current, total, message):
    print(f"{stage}: {current}/{total} - {message}")

processor = DataProcessorLarge(
    progress_callback=my_progress_callback
)
```

## 性能优化建议

### 1. 文件格式选择

**CSV格式**（推荐）：
- ✅ 可以真正的流式读取
- ✅ 内存占用小
- ✅ 处理速度快

**Excel格式**：
- ⚠️ 需要先读取到内存
- ⚠️ 内存占用大
- ⚠️ 处理速度较慢

**建议**：大文件（>200MB）转换为CSV格式

### 2. 批次大小调整

```python
# 内存充足（16GB+）
processor = DataProcessorLarge(chunk_size=10000)

# 内存中等（8-16GB）
processor = DataProcessorLarge(chunk_size=5000)

# 内存较小（<8GB）
processor = DataProcessorLarge(chunk_size=2000)
```

### 3. 数据库优化

```python
# 在data_loader中使用较小的数据库批次
loader = DataLoader(chunk_size=1000)  # 数据库批次稍小
```

## 内存管理

系统会自动进行内存管理：

1. **分块处理**：每次只处理一部分数据
2. **及时释放**：处理完一个批次后立即释放内存
3. **垃圾回收**：强制进行垃圾回收

### 内存使用估算

```
估算公式：
内存占用 ≈ 批次大小 × 每行数据大小 × 3倍（处理过程中的临时数据）

示例：
- 批次大小：5000行
- 每行数据：2KB
- 内存占用 ≈ 5000 × 2KB × 3 = 30MB（每个批次）
```

## 处理流程

大文件处理流程：

```
1. 文件检测
   ↓
2. 分块读取（每次读取chunk_size行）
   ↓
3. 数据清洗（对当前批次）
   ↓
4. 数据入库（批量插入）
   ↓
5. 释放内存
   ↓
6. 重复步骤2-5直到处理完成
```

## 常见问题

### Q1: 处理600M文件需要多长时间？

**A**: 取决于：
- 文件格式（CSV更快）
- 数据行数
- 硬件配置（CPU、内存、SSD）
- 批次大小

估算：600M CSV文件，约50-100万行，预计需要10-30分钟。

### Q2: 内存不足怎么办？

**A**: 
1. 减小批次大小（chunk_size）
2. 关闭其他程序释放内存
3. 增加系统内存
4. 将Excel转换为CSV

### Q3: Excel文件读取太慢？

**A**: 
1. 转换为CSV格式（推荐）
2. 减小批次大小
3. 使用SSD存储文件
4. 增加系统内存

### Q4: 处理中断了怎么办？

**A**: 
- 系统支持断点续传（使用append模式）
- 已处理的数据不会丢失
- 重新运行会继续追加数据

## 监控和调试

### 查看处理进度

```python
processor = DataProcessorLarge(
    verbose=True,  # 显示详细日志
    progress_callback=my_callback  # 自定义进度回调
)
```

### 查看内存使用

```python
import psutil
import os

process = psutil.Process(os.getpid())
memory_mb = process.memory_info().rss / 1024 / 1024
print(f"当前内存使用: {memory_mb:.1f} MB")
```

### 查看处理统计

```python
result = processor.process_large_file(...)
print(f"总读取行数: {result['total_rows_read']}")
print(f"总清洗行数: {result['total_rows_cleaned']}")
print(f"总入库行数: {result['total_rows_loaded']}")
print(f"处理批次数: {result['chunks_processed']}")
```

## 最佳实践

1. **预处理**：
   - 大Excel文件转换为CSV
   - 清理不需要的数据
   - 检查数据格式

2. **处理时**：
   - 使用合适的批次大小
   - 监控内存使用
   - 保持系统稳定运行

3. **验证**：
   - 检查入库数据量
   - 验证数据完整性
   - 查看错误日志

4. **优化**：
   - 根据实际情况调整批次大小
   - 使用SSD存储文件
   - 增加系统内存

