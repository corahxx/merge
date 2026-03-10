# 充电桩数据管理系统 - 部署运行文档

## 📋 目录

1. [系统要求](#系统要求)
2. [环境准备](#环境准备)
3. [数据库配置](#数据库配置)
4. [安装部署](#安装部署)
5. [运行启动](#运行启动)
6. [配置说明](#配置说明)
7. [常见问题](#常见问题)
8. [附录](#附录)

---

## 系统要求

### 硬件要求

- **CPU**: 4核或以上（推荐）
- **内存**: 8GB 或以上（处理大文件时建议16GB+）
- **磁盘空间**: 至少5GB可用空间（用于虚拟环境和依赖包）
- **网络**: 能够访问数据库服务器

### 软件要求

- **操作系统**: 
  - Windows 10/11
  - Linux (Ubuntu 18.04+, CentOS 7+)
  - macOS 10.14+
- **Python**: 3.11 或更高版本
- **MySQL**: 5.7+ 或 MySQL 8.0+
- **Git**: （可选，用于版本控制）

---

## 环境准备

### 1. 安装 Python

**Windows:**
1. 访问 https://www.python.org/downloads/
2. 下载 Python 3.11 或更高版本
3. 安装时勾选 "Add Python to PATH"
4. 验证安装：
   ```bash
   python --version
   ```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip

# CentOS/RHEL
sudo yum install python311 python311-pip
```

**macOS:**
```bash
# 使用 Homebrew
brew install python@3.11
```

### 2. 安装 MySQL

**Windows:**
1. 下载 MySQL Installer: https://dev.mysql.com/downloads/installer/
2. 选择 "MySQL Server" 安装
3. 记住 root 密码

**Linux:**
```bash
# Ubuntu/Debian
sudo apt install mysql-server
sudo systemctl start mysql
sudo systemctl enable mysql

# CentOS/RHEL
sudo yum install mysql-server
sudo systemctl start mysqld
sudo systemctl enable mysqld
```

**macOS:**
```bash
brew install mysql
brew services start mysql
```

### 3. 创建数据库

```sql
-- 登录 MySQL
mysql -u root -p

-- 创建数据库
CREATE DATABASE evcipadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建用户（可选，建议使用独立用户）
CREATE USER 'evdata_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON evcipadata.* TO 'evdata_user'@'localhost';
FLUSH PRIVILEGES;
```

---

## 数据库配置

### 修改配置文件

编辑项目根目录下的 `config.py` 文件：

```python
DB_CONFIG = {
    'host': 'localhost',        # 数据库主机地址
    'user': 'root',             # 数据库用户名
    'password': 'your_password', # 数据库密码
    'database': 'evcipadata'    # 数据库名称
}
```

**注意事项：**
- 如果是远程数据库，将 `host` 改为数据库服务器IP地址
- 确保数据库用户有足够的权限（SELECT, INSERT, UPDATE, DELETE）
- 建议使用独立用户而非 root

---

## 安装部署

### 1. 获取项目代码

```bash
# 如果有 Git 仓库
git clone <repository-url>
cd charging-agent

# 或者直接解压项目压缩包到目标目录
```

### 2. 创建虚拟环境（推荐）

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

**主要依赖包：**
- streamlit==1.32.0
- pandas==2.1.4
- PyMySQL==1.1.0
- sqlalchemy==2.0.23
- openpyxl==3.1.2
- xlrd==2.0.1
- plotly==5.18.0
- reportlab==4.0.7

### 4. 验证安装

```bash
# 检查 Streamlit 是否安装成功
streamlit --version

# 检查数据库连接（可选，需要先配置 config.py）
python -c "from config import DB_CONFIG; print(DB_CONFIG)"
```

---

## 运行启动

### 启动数据管理系统

**方式1：使用启动脚本（推荐）**

- **Windows**: 双击 `start_data_manager.bat`
- **Linux/macOS**: 
  ```bash
  chmod +x start_data_manager.sh
  ./start_data_manager.sh
  ```
  **启动充电桩型号查询（推荐）**
streamlit run pile_model_query.py
**方式2：直接命令启动**

```bash
# 激活虚拟环境后
streamlit run data_manager.py
```

**方式3：命令行参数启动（支持大文件）**

```bash
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600
```

启动成功后，浏览器会自动打开，默认地址为：`http://localhost:8501`

### 启动 AI 助手（可选）

```bash
# 推荐：使用main.py（带登录和侧边导航）
streamlit run main.py

# 或单独运行AI助手页面
# streamlit run app.py
```

**注意：** AI 助手功能需要先安装并运行 Ollama，详情参考 README.md

---

## 配置说明

### Streamlit 配置

项目已包含 Streamlit 配置文件 `.streamlit/config.toml`：

```toml
[server]
# 文件上传大小限制（MB）
maxUploadSize = 600

# WebSocket消息大小限制（MB）
maxMessageSize = 600

# 允许运行未签名的应用程序
enableXsrfProtection = false

# 启用CORS
enableCORS = true
```

如需修改配置，编辑 `.streamlit/config.toml` 文件。

### 数据表配置

系统支持动态表选择，默认表结构定义在 `data/table_schema.py` 中。

主要字段包括：
- 充电桩编号、充电桩内部编号
- 充电站名称、位置、投入使用时间
- 运营商信息
- 充电桩类型、属性、生产日期
- 额定功率、电压、电流等参数
- UID（唯一标识符，自动生成）

---

## Pandas 全量模式

### 什么是 Pandas 全量模式？

系统使用 `PandasDataService` 单例服务，在首次访问时将全量数据（约409万条）加载到内存中。后续所有查询操作直接在内存中进行，响应速度从秒级提升到毫秒级。

### 性能对比

| 操作 | SQL查询 | Pandas全量 | 提升 |
|------|---------|------------|------|
| 首次加载 | 0秒 | 30秒 | - |
| 数据预览 | 0.5秒 | 1ms | 500x |
| 统计分析 | 5-10秒 | 10ms | 500-1000x |
| 切换筛选 | 2-5秒 | 5ms | 400-1000x |

### 内存需求

- **数据量**: 409万条 × 12字段
- **内存占用**: 约 3.25GB
- **推荐配置**: 16GB+ 内存（32GB 最佳）

### 缓存管理

在侧边栏的"数据缓存"区域可以：
- 查看缓存状态（记录数、内存占用、缓存时间）
- 手动预加载数据
- 清除缓存

### 开发使用

```python
from services.pandas_data_service import PandasDataService

# 获取单例实例
service = PandasDataService.get_instance()

# 检查缓存状态
if service.is_loaded():
    print("缓存已加载")

# 获取全量数据（自动管理缓存）
df = service.get_dataframe()

# 随机预览
preview = service.preview(10)

# 统计分析（带筛选）
stats = service.get_statistics(filters={
    'province': '北京市',
    'operators': ['特来电', '星星充电']
})

# 数据变更后清除缓存
service.clear_cache()
```

---

## 常见问题

### 1. 文件上传失败

**问题**: 上传文件时提示文件过大

**解决方案**:
- 检查文件大小是否超过 600MB
- 确认 `.streamlit/config.toml` 中的 `maxUploadSize` 配置正确
- 使用启动脚本启动（已包含大文件支持参数）

### 2. 数据库连接失败

**问题**: 提示 "数据库连接失败"

**解决方案**:
- 检查 `config.py` 中的数据库配置是否正确
- 确认 MySQL 服务正在运行：
  - Windows: 服务管理器查看 MySQL 服务
  - Linux: `sudo systemctl status mysql`
- 测试数据库连接：
  ```bash
  mysql -u root -p -h localhost
  ```
- 检查数据库用户权限
- 如果使用远程数据库，检查防火墙设置

### 3. 导入数据行数为0

**问题**: 数据导入后，实际入库行数为 0

**可能原因和解决方案**:
- **数据类型不匹配**: 查看数据清洗日志，检查字段类型转换
- **字段名不匹配**: 确认 Excel 列名与数据库字段名匹配，或使用字段映射
- **数据验证失败**: 查看清洗统计中的"删除无效"数量
- **重复数据被删除**: 查看清洗统计中的"删除重复"数量

### 4. Python 模块导入错误

**问题**: `ModuleNotFoundError: No module named 'xxx'`

**解决方案**:
- 确认虚拟环境已激活
- 重新安装依赖：`pip install -r requirements.txt`
- 检查 Python 版本是否符合要求（3.11+）

### 5. 端口被占用

**问题**: 启动时提示端口 8501 已被占用

**解决方案**:
- 查找占用端口的进程并关闭：
  - Windows: `netstat -ano | findstr :8501`
  - Linux/macOS: `lsof -i :8501`
- 或使用其他端口启动：
  ```bash
  streamlit run data_manager.py --server.port 8502
  ```

### 6. 内存不足

**问题**: 处理大文件时出现内存错误

**解决方案**:
- 减小批次大小（在界面上调整 chunk_size 滑块，推荐 5000-10000）
- 增加系统内存
- 将大文件拆分为多个小文件分别导入
- 对于超大文件（>500MB），建议转换为 CSV 格式

---

## 附录

### A. 完整项目结构

```
charging-agent/
├── agent.py                  # AI助手主类
├── app.py                    # AI助手Streamlit界面
├── data_manager.py           # 数据管理Streamlit界面（主入口）
├── config.py                 # 数据库配置文件
├── requirements.txt          # Python依赖包
│
├── core/                     # AI助手核心模块
│   ├── orchestrator.py      # 流程编排器
│   ├── sql_planner.py       # SQL查询规划器
│   ├── sql_generator.py     # SQL生成器
│   ├── intent_classifier.py # 意图分类器
│   ├── redirector.py        # 对话重定向器
│   ├── context_manager.py   # 上下文管理器
│   ├── ai_responder.py      # AI回复生成器
│   ├── query_executor.py    # 查询执行器
│   ├── response_formatter.py # 响应格式化器
│   ├── schema_inspector.py  # 数据库模式检查器
│   └── knowledge_base.py    # 知识库
│
├── data/                     # 数据处理模块
│   ├── data_processor.py    # 数据处理主类
│   ├── data_processor_large.py # 大文件处理器
│   ├── excel_reader.py      # Excel读取器
│   ├── excel_reader_large.py # 大文件Excel读取器
│   ├── data_cleaner.py      # 数据清洗器
│   ├── data_loader.py       # 数据加载器
│   ├── data_analyzer.py     # 数据分析器
│   ├── type_converter.py    # 数据类型转换器
│   ├── table_schema.py      # 数据库表结构定义
│   └── error_handler.py     # 错误处理器
│
├── handlers/                 # 数据管理Handler类（业务逻辑层）
│   ├── file_upload_handler.py      # 文件上传处理
│   ├── data_import_handler.py      # 数据导入处理
│   ├── data_preview_handler.py     # 数据预览处理（Pandas优化）
│   ├── data_analysis_handler.py    # 数据分析处理（Pandas优化）
│   ├── data_compare_handler.py     # 数据比对处理
│   ├── data_quality_handler.py     # 数据质量处理
│   ├── data_cleaning_handler.py    # 数据清洗处理（重复检测、地址校验）
│   └── pdf_report_handler.py       # PDF报告生成
│
├── services/                 # 🆕 共享服务层
│   ├── pandas_data_service.py      # Pandas全量数据服务（单例模式）
│   ├── user_service.py             # 用户管理服务
│   └── audit_service.py            # 审计日志服务
│
├── auth/                     # 认证授权模块
│   ├── authenticator.py            # 认证器
│   ├── permission_checker.py       # 权限检查器
│   ├── session_manager.py          # 会话管理
│   └── password_utils.py           # 密码工具
│
├── .streamlit/              # Streamlit配置
│   └── config.toml          # Streamlit配置文件（支持600MB上传）
│
├── uploads/                 # 上传文件目录（自动创建）
│
├── start_data_manager.bat   # Windows启动脚本
├── start_data_manager.sh    # Linux/Mac启动脚本
│
└── docs/                    # 文档目录
    ├── README.md            # 项目主文档
    ├── README_DATA.md       # 数据管理模块文档
    ├── DEPLOYMENT_GUIDE.md  # 本文档
    ├── ERROR_HANDLING_GUIDE.md
    ├── LARGE_FILE_GUIDE.md
    ├── TYPE_CONVERSION_GUIDE.md
    └── TROUBLESHOOTING.md
```

### B. 数据导入模式说明

1. **追加模式 (append)**
   - 直接添加新数据到表末尾
   - 不检查重复
   - 适合导入全新的数据

2. **替换模式 (replace)**
   - 清空表后导入新数据
   - **注意：会删除表中所有现有数据**
   - 适合完全替换数据

3. **更新/插入模式 (upsert)**
   - 根据唯一键（默认：充电桩编号）判断
   - 如果记录存在，则更新字段
   - 如果记录不存在，则插入新记录
   - 适合增量更新数据

### C. 数据清洗规则

系统会自动执行以下清洗操作：

1. **字段映射**: 将 Excel 列名映射到数据库字段名
2. **去除空白**: 去除字符串字段的前后空白
3. **字段标准化**:
   - 运营商名称标准化（使用知识库映射）
   - 地理位置标准化
   - 充电桩类型标准化
4. **数据类型转换**: 根据数据库字段类型自动转换
   - 整数类型：去除 `.0`，进行四舍五入
   - 日期类型：统一转换为 `YYYY-MM-DD` 格式（横线分隔）
   - VARCHAR类型：超出长度限制时从头截断
5. **空值处理**:
   - 运营商名称为空时，自动填充为 "无登记运营商"
   - 其他字段的空值保留为空
6. **去重处理**:
   - 基于"充电桩编号"去重（保留第一条）
   - **注意：充电桩编号为空的记录不参与去重，全部保留**
7. **数据验证**: 验证数据有效性

### D. 数据类型转换说明

#### 整数类型 (INT, INTEGER, BIGINT等)
- 自动去除小数点后的 `.0`
- 对浮点数进行四舍五入
- 超出范围的值会被拒绝（MySQL会报错）

#### 日期类型 (DATE)
- 统一转换为 `YYYY-MM-DD` 格式
- 只使用横线 `-` 分隔，不使用斜杠 `/`
- 无法转换的日期值设为 NULL
- 空字符串和无效值转换为 NULL

#### 字符串类型 (VARCHAR, CHAR, TEXT)
- 超出定义长度的字符串会从头截断
- 例如：VARCHAR(255) 字段，如果字符串长度为 300，会截取前 255 个字符

#### 示例
```
原始值: 2201001.0  → 转换后: 2201001
原始值: 2024/9/12  → 转换后: 2024-09-12
原始值: (空字符串) → 转换后: NULL (日期字段)
原始值: "很长的字符串..." (长度>255) → 转换后: "很长的字符串..." (前255个字符)
```

### E. 性能优化建议

1. **大文件处理**
   - 文件大小 > 100MB 时，建议使用较小的批次大小（5000-10000行）
   - 对于超大文件（>500MB），建议先转换为 CSV 格式
   - 分批导入可以提高成功率并降低内存占用

2. **数据库优化**
   - 在"充电桩编号"字段上创建索引（如果是唯一键）
   - 定期优化表：`OPTIMIZE TABLE evdata;`
   - 对于大批量导入，可以临时关闭索引，导入后重建

3. **系统资源**
   - 处理大文件时，关闭其他占用内存的程序
   - 如果内存不足，减小批次大小
   - 使用 SSD 硬盘可以提高处理速度

### F. 日志和调试

#### 日志文件位置

- **数据处理日志**: `data_processing.log`（项目根目录）
- **Streamlit 日志**: 在终端/命令行窗口显示

#### 启用详细日志

在代码中设置 `verbose=True` 可以查看详细的处理过程：

```python
from data.data_processor import DataProcessor

processor = DataProcessor(table_name='evdata', verbose=True)
```

#### 查看错误信息

- 在 Streamlit 界面中，错误信息会显示在相应的错误提示区域
- 查看 `data_processing.log` 文件获取详细错误堆栈
- 在终端查看 Streamlit 的实时日志输出

### G. 安全建议

1. **数据库安全**
   - 不要在生产环境使用 root 用户
   - 为应用创建独立的数据库用户
   - 使用强密码
   - 限制数据库用户的权限（只授予必要的权限）

2. **配置文件安全**
   - 不要将 `config.py` 提交到公开的代码仓库
   - 使用环境变量或配置文件管理敏感信息
   - 建议使用 `.gitignore` 忽略配置文件

3. **网络安全**
   - 如果部署在服务器上，使用 HTTPS
   - 配置防火墙规则，限制访问来源
   - 定期更新依赖包，修复安全漏洞

### H. 维护和更新

#### 更新依赖包

```bash
# 激活虚拟环境
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows

# 更新所有包到最新版本
pip install --upgrade -r requirements.txt

# 或更新单个包
pip install --upgrade streamlit
```

#### 备份数据库

```bash
# 备份数据库
mysqldump -u root -p evcipadata > backup_$(date +%Y%m%d).sql

# 恢复数据库
mysql -u root -p evcipadata < backup_20231218.sql
```

#### 清理临时文件

- `uploads/` 目录中的上传文件可以定期清理
- 日志文件 `data_processing.log` 可以定期归档或清理

---

## 联系支持

如有问题或需要技术支持，请联系项目维护团队。

---

**文档版本**: 1.0  
**最后更新**: 2024-12-18
