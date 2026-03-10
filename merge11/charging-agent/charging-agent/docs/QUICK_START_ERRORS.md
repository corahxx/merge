# 错误处理快速参考

## 🚨 遇到错误时立即执行

### 第一步：运行诊断工具

```bash
python test_data_system.py
```

这会快速检查所有可能的问题。

### 第二步：查看错误日志

```bash
# Windows
type data_processing.log

# Linux/Mac
tail -n 50 data_processing.log
```

### 第三步：根据错误类型查找解决方案

| 错误类型 | 快速解决方案 |
|---------|------------|
| **数据库连接失败** | 检查 `config.py`，确认数据库服务已启动 |
| **表不存在** | 使用 `if_exists='replace'` 模式 |
| **文件读取失败** | 检查文件路径和格式（.xlsx, .xls, .csv） |
| **编码错误** | 将文件另存为UTF-8编码 |
| **内存不足** | 减小文件大小或使用更小的 `chunk_size` |
| **字段不存在** | 检查字段映射配置 |
| **权限错误** | 检查数据库用户权限 |

## 📋 常用命令

### 测试数据库连接
```python
from sqlalchemy import create_engine
from config import DB_CONFIG

db_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
engine = create_engine(db_url)
with engine.connect() as conn:
    print("✅ 连接成功")
```

### 查看表结构
```python
from sqlalchemy import create_engine, inspect
from config import DB_CONFIG

db_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
engine = create_engine(db_url)
inspector = inspect(engine)
columns = inspector.get_columns('table2509ev')
for col in columns:
    print(f"{col['name']}: {col['type']}")
```

### 检查EXCEL文件
```python
from data.excel_reader import ExcelReader

reader = ExcelReader('file.xlsx')
info = reader.get_info()
print(f"行数: {info.get('rows', 'N/A')}")
print(f"列名: {info.get('column_names', [])}")
```

## 🔧 界面中的错误处理

在Streamlit界面中：

1. **查看简化错误消息** - 显示在界面上
2. **展开详细错误** - 点击"查看详细错误信息"
3. **查看解决方案** - 系统会自动提供建议
4. **运行诊断** - 点击侧边栏的"运行系统诊断"按钮

## 📚 详细文档

- **完整故障排除指南**: `TROUBLESHOOTING.md`
- **错误处理详细说明**: `ERROR_HANDLING_GUIDE.md`
- **系统使用文档**: `README_DATA.md`

## ⚡ 紧急修复清单

遇到错误时按顺序检查：

- [ ] 运行 `python test_data_system.py`
- [ ] 检查 `config.py` 配置
- [ ] 确认数据库服务运行
- [ ] 检查文件路径和格式
- [ ] 查看 `data_processing.log` 日志
- [ ] 尝试使用更小的测试数据
- [ ] 检查Python库是否安装完整：`pip install -r requirements.txt`

