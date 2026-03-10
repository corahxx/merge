# EXCEL数据导入和管理系统

## 功能概述

这是一个完整的EXCEL数据处理系统，支持：
- 📤 **自动读取EXCEL文档**（支持 .xlsx, .xls, .csv）
- 🧹 **数据清洗和标准化**（自动去重、空值处理、字段标准化）
- 💾 **数据入库**（支持追加、替换、更新/插入模式）
- 📊 **数据分析和统计**（按运营商、区域、类型等维度统计）
- 🔄 **数据比对**（比对两个EXCEL文件或与数据库数据比对）
- 📋 **数据质量报告**（检查空值、重复记录等）

## 安装依赖

```bash
pip install -r requirements.txt
```

主要新增依赖：
- pandas: 数据处理
- openpyxl: 读取.xlsx文件
- xlrd: 读取.xls文件
- sqlalchemy: 数据库操作

## 使用方法

### 1. 通过Streamlit界面使用

**启动数据管理界面（支持600MB文件上传）：**

**方式1：使用配置文件（推荐）**
```bash
streamlit run data_manager.py
```
配置文件 `.streamlit/config.toml` 已设置支持600MB文件上传。

**方式2：使用命令行参数**
```bash
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600
```

**方式3：使用启动脚本**
- Windows: 双击 `start_data_manager.bat`
- Linux/Mac: `bash start_data_manager.sh`

界面包含5个标签页：
- **数据导入**：上传EXCEL文件并导入数据库
- **数据预览**：预览数据库中的数据
- **数据分析**：生成各种统计报告
- **数据比对**：比对两个EXCEL文件
- **数据质量**：生成数据质量报告

### 2. 通过代码使用

```python
from data.data_processor import DataProcessor

# 初始化处理器
processor = DataProcessor(table_name='evdata', verbose=True)

# 处理单个EXCEL文件
result = processor.process_excel_file(
    file_path='data.xlsx',
    field_mapping={
        'EXCEL列名1': '数据库字段名1',
        'EXCEL列名2': '数据库字段名2',
    },
    if_exists='append',  # 或 'replace', 使用 'upsert' 需要设置 use_upsert=True
    use_upsert=False,
    unique_key='充电桩编号'
)

# 批量处理多个文件
results = processor.process_multiple_files(
    file_paths=['file1.xlsx', 'file2.xlsx'],
    field_mapping={...}
)

# 比对两个文件
comparison = processor.compare_files('file1.xlsx', 'file2.xlsx')

# 获取数据库统计
stats = processor.get_database_statistics(group_by='运营商名称')

# 生成数据质量报告
quality_report = processor.get_data_quality_report()
```

## 模块说明

### ExcelReader (data/excel_reader.py)
- 读取EXCEL文件（.xlsx, .xls, .csv）
- 支持选择工作表
- 获取文件基本信息

### DataCleaner (data/data_cleaner.py)
- 数据清洗和标准化
- 使用知识库标准化运营商名称、地理位置等
- 去重、空值处理、数据验证

### DataLoader (data/data_loader.py)
- 将数据导入MySQL数据库
- 支持追加、替换、更新/插入模式
- 分批导入，提高性能

### DataAnalyzer (data/data_analyzer.py)
- 数据比对和整合
- 统计分析
- 数据质量检查

### DataProcessor (data/data_processor.py)
- 整合所有功能的主类
- 提供一站式数据处理流程

## 字段映射

如果EXCEL文件的列名与数据库字段名不一致，可以通过字段映射来转换：

```python
field_mapping = {
    '桩编号': '充电桩编号',
    '运营商': '运营商名称',
    '位置': '充电站位置',
    '类型': '充电桩类型',
    # ...
}
```

## 导入模式说明

1. **追加模式 (append)**: 直接添加新数据到表末尾
2. **替换模式 (replace)**: 清空表后导入新数据
3. **更新/插入模式 (upsert)**: 
   - 如果记录存在（根据唯一键判断），则更新
   - 如果记录不存在，则插入

## 数据清洗功能

系统会自动进行以下清洗操作：
- ✅ 去除字符串前后空白
- ✅ 标准化运营商名称（使用知识库映射）
- ✅ 标准化地理位置（使用知识库映射）
- ✅ 标准化充电桩类型
- ✅ 处理时间字段格式
- ✅ 删除关键字段为空的记录
- ✅ 去除重复记录

## 注意事项

1. 确保数据库连接配置正确（config.py）
2. 确保目标表存在或使用replace模式自动创建
3. 大文件建议分批处理，避免内存溢出
4. 使用upsert模式时，确保唯一键字段存在且正确

## 示例

完整的使用示例请参考 `data_manager.py` 中的Streamlit界面实现。

