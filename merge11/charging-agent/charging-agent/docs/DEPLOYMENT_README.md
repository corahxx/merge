# Windows 离线部署指南

本指南说明如何在Windows服务器（无网络环境）上部署充电桩数据管理系统。

## 📋 目录

1. [部署前准备](#部署前准备)
2. [准备离线安装包](#准备离线安装包)
3. [服务器部署](#服务器部署)
4. [启动应用](#启动应用)
5. [常见问题](#常见问题)

---

## 部署前准备

### 系统要求

- **操作系统**: Windows Server 2012+ 或 Windows 10/11
- **Python**: 3.11 或更高版本
- **内存**: 建议 4GB 以上
- **磁盘空间**: 至少 2GB 可用空间

### 需要准备的文件

1. 项目源代码（整个项目文件夹）
2. Python 3.11+ 安装包（如果服务器未安装）
3. 离线安装包（wheels目录，可选）

---

## 准备离线安装包

### 方法1：使用准备脚本（推荐）

在有网络的机器上：

1. 将项目文件夹复制到有网络的机器
2. 打开命令提示符，进入项目目录
3. 运行准备脚本：

```batch
prepare_offline_packages.bat
```

脚本会自动：
- 检查Python环境
- 创建wheels目录
- 下载所有依赖包的wheel文件
- 显示下载统计信息

### 方法2：手动下载

如果有网络但想手动控制：

```batch
# 创建wheels目录
mkdir wheels

# 下载所有依赖包
pip download -r requirements.txt -d wheels
```

### 下载内容说明

下载完成后，`wheels` 目录将包含：
- 所有依赖包的 `.whl` 文件
- 依赖包的依赖包（递归下载）

**注意**: wheels目录可能较大（约500MB-1GB），请确保有足够空间。

---

## 服务器部署

### 步骤1：复制项目文件

将整个项目文件夹复制到目标服务器，确保包含：
- 所有源代码文件
- `requirements.txt`
- `deploy_windows.bat`（部署脚本）
- `wheels` 目录（如果已准备离线包）

### 步骤2：安装Python（如未安装）

1. 下载Python 3.11+安装包（.exe文件）
2. 运行安装程序
3. **重要**: 安装时勾选 "Add Python to PATH"
4. 验证安装：

```batch
python --version
```

### 步骤3：运行部署脚本

1. 进入项目目录
2. 双击运行 `deploy_windows.bat`
3. 按照提示完成部署

部署脚本会自动：
- ✅ 检查Python环境
- ✅ 创建虚拟环境（如不存在）
- ✅ 安装依赖包（从离线包或requirements.txt）
- ✅ 检查/创建配置文件
- ✅ 创建必要目录
- ✅ 启动应用

### 部署过程说明

#### 步骤1: 检查Python环境
```
[1/6] 检查Python环境...
   ✓ Python已安装: 3.11.5
   ✓ Python版本检查通过
```

#### 步骤2: 检查虚拟环境
```
[2/6] 检查虚拟环境...
   ✓ 虚拟环境已存在: D:\charging-agent\venv
```

#### 步骤3: 检查依赖包
```
[3/6] 检查依赖包...
   正在升级pip...
   检查依赖包安装状态...
   ✓ 依赖包已安装
```

如果依赖包未安装，脚本会：
- 首先尝试从 `wheels` 目录安装（离线模式）
- 如果失败，尝试从网络安装（如果有网络）
- 如果都失败，显示详细说明

#### 步骤4: 检查配置文件
```
[4/6] 检查配置文件...
   ✓ 配置文件已存在: D:\charging-agent\config.py
   ✓ 配置文件格式正确
```

如果配置文件不存在，脚本会创建默认配置，并询问是否立即配置数据库。

#### 步骤5: 创建必要目录
```
[5/6] 创建必要目录...
   ✓ 上传目录已存在: D:\charging-agent\uploads
   ✓ Streamlit配置文件已存在
```

#### 步骤6: 选择启动应用
```
[6/6] 准备启动应用...

   请选择要启动的应用:

   1. 数据管理系统 (data_manager.py)
   2. 充电桩型号查询 (pile_model_query.py)
   3. AI助手 (app.py - 需要Ollama)
   4. 退出

   请输入选项 (1-4):
```

---

## 启动应用

### 方式1：使用部署脚本启动

运行 `deploy_windows.bat`，选择要启动的应用。

### 方式2：使用启动脚本

如果部署已完成，可以直接使用：

```batch
# 启动数据管理系统
start_data_manager.bat

# 或手动启动
venv\Scripts\activate
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600
```

### 方式3：命令行启动

```batch
# 激活虚拟环境
venv\Scripts\activate

# 启动应用
streamlit run data_manager.py
streamlit run pile_model_query.py
streamlit run app.py
```

### 访问应用

启动成功后，浏览器会自动打开，或手动访问：
- **默认地址**: http://localhost:8501
- **局域网访问**: http://服务器IP:8501

---

## 配置说明

### 数据库配置

编辑 `config.py` 文件：

```python
DB_CONFIG = {
    'host': 'localhost',        # 数据库主机
    'user': 'root',             # 数据库用户名
    'password': 'your_password', # 数据库密码
    'database': 'evcipadata'    # 数据库名称
}
```

### Streamlit配置

配置文件位于 `.streamlit/config.toml`：

```toml
[server]
maxUploadSize = 600      # 最大上传文件大小(MB)
maxMessageSize =600      # 最大消息大小(MB)
enableXsrfProtection = false
enableCORS = true
```

### 防火墙配置

如果需要在局域网访问，需要：

1. **Windows防火墙**:
   - 打开"Windows Defender 防火墙"
   - 点击"高级设置"
   - 添加入站规则，允许端口8501

2. **服务器网络**:
   - 确保服务器IP可访问
   - 检查路由器端口转发（如需要）

---

## 常见问题

### 1. Python未找到

**问题**: 提示"未找到Python环境"

**解决方案**:
- 确认Python已安装
- 检查PATH环境变量是否包含Python
- 重新安装Python，勾选"Add Python to PATH"
- 或手动添加Python到PATH

### 2. 依赖包安装失败（离线环境）

**问题**: 提示"依赖包安装失败"

**解决方案**:

**方案A: 使用离线安装包**
1. 在有网络的机器上运行 `prepare_offline_packages.bat`
2. 将生成的 `wheels` 文件夹复制到服务器
3. 重新运行部署脚本

**方案B: 手动准备wheel包**
```batch
# 在有网络的机器上
pip download -r requirements.txt -d wheels

# 复制wheels目录到服务器
# 在服务器上
pip install --no-index --find-links=wheels -r requirements.txt
```

### 3. 虚拟环境创建失败

**问题**: 提示"创建虚拟环境失败"

**解决方案**:
- 检查Python安装是否完整
- 确认有足够的磁盘空间
- 尝试手动创建：`python -m venv venv`
- 检查是否有杀毒软件阻止

### 4. 数据库连接失败

**问题**: 应用启动后提示数据库连接失败

**解决方案**:
- 检查 `config.py` 配置是否正确
- 确认MySQL服务正在运行
- 测试数据库连接：
  ```batch
  mysql -u root -p -h localhost
  ```
- 检查数据库用户权限
- 确认数据库已创建

### 5. 端口被占用

**问题**: 提示端口8501已被占用

**解决方案**:
- 查找占用端口的进程：
  ```batch
  netstat -ano | findstr :8501
  ```
- 结束占用进程或更改Streamlit端口：
  ```batch
  streamlit run data_manager.py --server.port 8502
  ```

### 6. 文件上传失败

**问题**: 上传文件时提示文件过大

**解决方案**:
- 检查文件大小是否超过600MB
- 确认 `.streamlit/config.toml` 配置正确
- 使用启动脚本启动（包含大文件参数）

### 7. 应用无法访问

**问题**: 浏览器无法打开应用

**解决方案**:
- 检查应用是否正常启动（查看命令行输出）
- 确认防火墙允许端口8501
- 尝试使用 `http://127.0.0.1:8501` 访问
- 检查是否有其他应用占用端口

---

## 维护和更新

### 更新依赖包

如果有网络连接：

```batch
venv\Scripts\activate
pip install --upgrade -r requirements.txt
```

如果无网络，需要：
1. 在有网络的机器上准备新的wheel包
2. 复制到服务器
3. 使用 `--no-index --find-links=wheels` 安装

### 备份配置

建议定期备份：
- `config.py` - 数据库配置
- `.streamlit/config.toml` - Streamlit配置
- `uploads/` - 上传的文件（如需要）

### 日志查看

应用日志位置：
- Streamlit日志：查看命令行输出
- 数据处理日志：`data_processing.log`

---

## 快速参考

### 部署命令

```batch
# 准备离线包（有网络机器）
prepare_offline_packages.bat

# 部署应用（服务器）
deploy_windows.bat

# 启动应用
start_data_manager.bat
```

### 常用目录

```
charging-agent/
├── venv/              # 虚拟环境
├── wheels/            # 离线安装包
├── uploads/            # 上传文件目录
├── .streamlit/        # Streamlit配置
├── config.py          # 数据库配置
└── requirements.txt   # 依赖列表
```

### 环境变量

如果需要，可以设置：
- `STREAMLIT_SERVER_PORT` - Streamlit端口
- `STREAMLIT_SERVER_ADDRESS` - 监听地址

---

## 技术支持

如遇到问题：
1. 查看本文档的"常见问题"部分
2. 检查应用日志输出
3. 联系技术支持

---

**最后更新**: 2025年

