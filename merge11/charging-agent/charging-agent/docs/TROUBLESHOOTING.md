# 故障排除指南

## 快速诊断

如果遇到错误，首先运行诊断工具：

```bash
python test_data_system.py
```

这个工具会自动检查：
- ✅ 数据库连接
- ✅ 必要的Python库
- ✅ 数据模块导入
- ✅ 知识库功能
- ✅ 配置完整性

## 常见错误及解决方案

### 1. 数据库连接错误

**错误信息示例：**
```
mysql+pymysql错误
Access denied for user
Can't connect to MySQL server
```

**解决方案：**
1. 检查数据库服务是否启动
   ```bash
   # Windows
   net start MySQL
   
   # Linux/Mac
   sudo systemctl start mysql
   ```

2. 检查 `config.py` 中的配置
   ```python
   DB_CONFIG = {
       'host': 'localhost',      # 确认主机地址
       'user': 'root',           # 确认用户名
       'password': 'your_password',  # 确认密码
       'database': 'evcipadata'  # 确认数据库名
   }
   ```

3. 测试数据库连接
   ```python
   import pymysql
   conn = pymysql.connect(
       host='localhost',
       user='root',
       password='your_password',
       database='evcipadata'
   )
   ```

### 2. 表不存在错误

**错误信息示例：**
```
Table 'evcipadata.table2509ev' doesn't exist
```

**解决方案：**
1. 使用 `replace` 模式自动创建表
   ```python
   processor.process_excel_file('file.xlsx', if_exists='replace')
   ```

2. 或者手动创建表
   ```sql
   CREATE TABLE table2509ev (
       充电桩编号 VARCHAR(100),
       充电桩类型 VARCHAR(50),
       充电站名称 VARCHAR(200),
       充电站位置 VARCHAR(200),
       运营商名称 VARCHAR(100),
       区县_中文 VARCHAR(100),
       充电开始时间 DATETIME
   );
   ```

### 3. 文件读取错误

**错误信息示例：**
```
FileNotFoundError
Permission denied
Unsupported file format
```

**解决方案：**
1. 检查文件路径是否正确（使用绝对路径）
2. 检查文件是否被其他程序打开
3. 确认文件格式支持（.xlsx, .xls, .csv）
4. 检查文件权限

### 4. 编码错误

**错误信息示例：**
```
UnicodeDecodeError
'utf-8' codec can't decode
```

**解决方案：**
1. 将EXCEL文件另存为UTF-8编码
2. 如果文件中有特殊字符，尝试：
   ```python
   # 在excel_reader.py中修改编码
   self.df = pd.read_csv(
       self.file_path,
       encoding='gbk'  # 或 'gb2312', 'utf-8-sig'
   )
   ```

### 5. 内存不足错误

**错误信息示例：**
```
MemoryError
Out of memory
```

**解决方案：**
1. 减小数据文件大小（分批处理）
2. 使用更小的 `chunk_size`
   ```python
   processor.process_excel_file(
       'file.xlsx',
       chunk_size=500  # 减小批次大小
   )
   ```
3. 关闭其他占用内存的程序
4. 增加系统内存或使用更大内存的机器

### 6. 字段映射错误

**错误信息示例：**
```
Column 'xxx' not found
Field mapping error
```

**解决方案：**
1. 检查EXCEL文件中的列名
   ```python
   # 先读取文件查看列名
   from data.excel_reader import ExcelReader
   reader = ExcelReader('file.xlsx')
   df = reader.read()
   print(df.columns.tolist())
   ```

2. 正确配置字段映射
   ```python
   field_mapping = {
       'EXCEL中的列名': '数据库字段名',
       '桩编号': '充电桩编号',
       '运营商': '运营商名称'
   }
   ```

### 7. 数据类型错误

**错误信息示例：**
```
TypeError
ValueError: invalid literal
```

**解决方案：**
1. 检查数据类型是否正确
2. 清理数据中的无效值
3. 使用数据清洗功能自动处理

### 8. 权限错误

**错误信息示例：**
```
Access denied
Permission denied
```

**解决方案：**
1. 检查数据库用户权限
   ```sql
   GRANT ALL PRIVILEGES ON evcipadata.* TO 'your_user'@'localhost';
   FLUSH PRIVILEGES;
   ```

2. 检查文件读写权限
3. 以管理员身份运行程序

## 调试技巧

### 1. 查看详细日志

所有错误都会记录到 `data_processing.log` 文件中：

```bash
# Windows
type data_processing.log

# Linux/Mac
cat data_processing.log
```

### 2. 启用详细输出

使用 `verbose=True` 参数查看详细处理过程：

```python
processor = DataProcessor(verbose=True)
```

### 3. 分步测试

分别测试各个组件：

```python
# 测试文件读取
from data.excel_reader import ExcelReader
reader = ExcelReader('file.xlsx')
df = reader.read()
print(df.head())

# 测试数据清洗
from data.data_cleaner import DataCleaner
cleaner = DataCleaner(verbose=True)
df_cleaned = cleaner.clean(df)
print(df_cleaned.head())

# 测试数据入库
from data.data_loader import DataLoader
loader = DataLoader(verbose=True)
result = loader.load(df_cleaned)
print(result)
```

### 4. 检查数据质量

在处理前先检查数据：

```python
from data.excel_reader import ExcelReader

reader = ExcelReader('file.xlsx')
df = reader.read()

# 查看基本信息
print(f"行数: {len(df)}")
print(f"列数: {len(df.columns)}")
print(f"列名: {list(df.columns)}")
print(f"空值统计:\n{df.isnull().sum()}")
print(f"重复行数: {df.duplicated().sum()}")
```

## 获取帮助

如果问题仍然无法解决：

1. **查看完整错误信息**
   - 运行诊断工具: `python test_data_system.py`
   - 查看日志文件: `data_processing.log`
   - 查看错误堆栈跟踪

2. **收集信息**
   - Python版本: `python --version`
   - 已安装的包: `pip list`
   - 错误消息的完整内容
   - 数据文件的基本信息（行数、列数等）

3. **尝试最小示例**
   - 使用小数据集测试
   - 简化字段映射
   - 使用基本的导入模式

## 预防措施

1. **定期备份数据**
2. **在测试环境先验证**
3. **使用小批量数据测试**
4. **检查数据文件格式**
5. **确认数据库配置正确**
6. **保持依赖库更新**

