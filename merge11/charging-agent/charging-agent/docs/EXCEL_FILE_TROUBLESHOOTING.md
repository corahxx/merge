# Excel文件读取问题排查指南

## EOFError 错误

### 错误原因

`EOFError` 在读取Excel文件时通常表示：
1. **文件损坏或不完整** - 文件在传输/复制过程中被截断
2. **文件正在被使用** - Excel或其他程序正在打开该文件
3. **文件未完全下载** - 下载过程中断
4. **磁盘空间不足** - 读取过程中磁盘空间耗尽
5. **内存不足** - 系统内存不足以处理大文件

### 解决方案

#### 方案1：检查文件完整性（推荐）

1. **在Excel中打开文件**
   ```
   - 双击文件，看是否能正常打开
   - 如果能打开，说明文件本身没问题
   - 尝试"另存为"新文件
   ```

2. **检查文件大小**
   ```python
   import os
   file_size = os.path.getsize('your_file.xlsx')
   print(f"文件大小: {file_size / 1024 / 1024:.2f} MB")
   ```
   如果文件大小异常小（比如只有几KB），说明文件可能不完整。

3. **验证ZIP结构**（XLSX文件是ZIP格式）
   ```python
   import zipfile
   try:
       with zipfile.ZipFile('your_file.xlsx', 'r') as zf:
           zf.testzip()  # 如果返回None，说明文件完整
   except Exception as e:
       print(f"文件损坏: {e}")
   ```

#### 方案2：转换为CSV格式（强烈推荐）

**为什么推荐CSV：**
- ✅ 可以流式读取，不需要一次性加载到内存
- ✅ 处理速度更快
- ✅ 内存占用更小
- ✅ 避免Excel格式问题

**转换方法：**

1. **使用Excel转换**
   ```
   1. 在Excel中打开文件
   2. 文件 -> 另存为
   3. 选择"CSV UTF-8 (逗号分隔)(*.csv)"
   4. 保存
   ```

2. **使用Python转换**
   ```python
   import pandas as pd
   
   # 读取Excel（如果文件损坏，这里会报错）
   try:
       df = pd.read_excel('large_file.xlsx', nrows=1000)  # 先读取少量数据测试
       print("文件可以读取")
       
       # 如果文件很大，分批读取并保存为CSV
       chunk_size = 10000
       for i, chunk in enumerate(pd.read_excel('large_file.xlsx', chunksize=chunk_size)):
           chunk.to_csv(f'output_chunk_{i}.csv', index=False, encoding='utf-8-sig')
   except Exception as e:
       print(f"文件无法读取: {e}")
   ```

#### 方案3：修复文件

1. **使用Excel修复**
   ```
   1. 打开Excel
   2. 文件 -> 打开
   3. 选择损坏的文件
   4. 点击"打开"旁边的下拉箭头
   5. 选择"打开并修复"
   ```

2. **使用在线工具**
   - 搜索"Excel文件修复工具"
   - 上传文件进行修复

#### 方案4：检查系统资源

1. **关闭其他程序**
   - 关闭Excel程序
   - 关闭其他占用内存的程序
   - 确保文件没有被其他程序锁定

2. **检查磁盘空间**
   ```bash
   # Windows
   dir C:\
   
   # 或使用Python
   import shutil
   free_space = shutil.disk_usage('.').free / 1024 / 1024 / 1024
   print(f"可用空间: {free_space:.2f} GB")
   ```

3. **检查内存**
   ```python
   import psutil
   memory = psutil.virtual_memory()
   print(f"可用内存: {memory.available / 1024 / 1024 / 1024:.2f} GB")
   ```

## 预防措施

### 1. 文件传输时
- ✅ 使用可靠的传输方式
- ✅ 验证文件完整性（MD5/SHA256校验）
- ✅ 避免在网络不稳定时传输大文件

### 2. 文件存储时
- ✅ 定期备份
- ✅ 使用稳定的存储介质
- ✅ 避免在文件打开时进行复制/移动操作

### 3. 处理大文件时
- ✅ 优先使用CSV格式
- ✅ 分批处理
- ✅ 监控系统资源

## 快速诊断脚本

```python
# check_excel_file.py
import os
import zipfile
import pandas as pd
from pathlib import Path

def check_excel_file(file_path):
    """检查Excel文件完整性"""
    file_path = Path(file_path)
    
    print(f"检查文件: {file_path.name}")
    print(f"文件大小: {file_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    # 检查1: 文件是否存在
    if not file_path.exists():
        print("❌ 文件不存在")
        return False
    
    # 检查2: 文件大小
    if file_path.stat().st_size == 0:
        print("❌ 文件大小为0，文件可能损坏")
        return False
    
    # 检查3: ZIP结构（XLSX）
    if file_path.suffix == '.xlsx':
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                bad_file = zf.testzip()
                if bad_file:
                    print(f"❌ ZIP文件损坏: {bad_file}")
                    return False
                else:
                    print("✅ ZIP结构正常")
        except Exception as e:
            print(f"❌ 无法打开ZIP文件: {e}")
            return False
    
    # 检查4: 尝试读取少量数据
    try:
        df = pd.read_excel(file_path, nrows=10)
        print(f"✅ 可以读取数据，共 {len(df)} 列")
        print(f"   列名: {list(df.columns)[:5]}...")
        return True
    except EOFError:
        print("❌ EOFError: 文件可能不完整或损坏")
        return False
    except Exception as e:
        print(f"❌ 读取失败: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        check_excel_file(sys.argv[1])
    else:
        print("用法: python check_excel_file.py <文件路径>")
```

## 使用建议

对于600M+的大文件：

1. **强烈建议转换为CSV**
   - 处理速度提升10倍以上
   - 内存占用减少50%以上
   - 避免Excel格式问题

2. **如果必须使用Excel**
   - 确保文件完整性
   - 关闭所有Excel程序
   - 增加系统内存
   - 使用较小的批次大小

3. **处理前验证**
   ```python
   # 先检查文件
   from data.excel_reader_large import ExcelReaderLarge
   
   reader = ExcelReaderLarge('your_file.xlsx')
   if reader._verify_file_integrity():
       print("文件完整性检查通过")
   else:
       print("文件可能损坏，请检查")
   ```

