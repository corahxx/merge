# 充电桩数据管理系统 - EXE 快速部署指南

> **版本**: 2.0  
> **更新日期**: 2026-01-14  
> **适用场景**: 在新机器上快速部署系统

---

## 📋 目录

1. [部署前检查清单](#一部署前检查清单)
2. [需要复制的文件](#二需要复制的文件)
3. [部署步骤](#三部署步骤)
4. [首次登录](#四首次登录)
5. [常见问题](#五常见问题)
6. [验证部署](#六验证部署成功)

---

## 一、部署前检查清单

### 1.1 目标机器要求

| 项目 | 要求 | 检查命令 |
|------|------|----------|
| **操作系统** | Windows 10/11 64位 | `winver` |
| **内存** | ≥8GB（推荐16GB） | 任务管理器 |
| **磁盘空间** | ≥500MB 可用空间 | - |
| **网络** | 能访问数据库服务器 | `ping <数据库IP>` |

### 1.2 数据库准备

| 项目 | 说明 |
|------|------|
| **MySQL 版本** | 5.7+ 或 8.0+ |
| **数据库名** | `evcipadata`（或自定义） |
| **端口** | 默认 3306 |

**需要的数据库表：**
```
evdata          - 充电桩数据主表
sys_users       - 用户账号表
sys_roles       - 角色表
sys_audit_logs  - 操作审计日志
sys_login_logs  - 登录日志
```

---

## 二、需要复制的文件

### 2.1 完整部署包（推荐）

```
📁 charging-agent/
│
├── 🚀 启动器
│   └── charging-agent-launcher.exe    # ⭐ 必需
│
├── 📄 配置文件
│   └── config.py                       # ⭐ 必需（首次启动会自动生成）
│
├── 📄 主程序文件
│   ├── main.py                         # ⭐ 必需 - 主入口
│   ├── app.py                          # ⭐ 必需 - AI助手
│   ├── data_manager.py                 # ⭐ 必需 - 数据管理
│   ├── pile_model_query.py             # ⭐ 必需 - 充电桩查询
│   └── agent.py                        # ⭐ 必需 - AI智能体
│
├── 📁 核心模块（整个文件夹）
│   ├── auth/                           # ⭐ 必需 - 认证模块
│   ├── services/                       # ⭐ 必需 - 服务层
│   ├── pages/                          # ⭐ 必需 - 管理页面
│   ├── core/                           # ⭐ 必需 - AI核心
│   ├── data/                           # ⭐ 必需 - 数据处理
│   ├── handlers/                       # ⭐ 必需 - 业务处理
│   └── utils/                          # ⭐ 必需 - 工具函数
│
├── 📁 静态资源
│   ├── assets/                         # 推荐 - 包含LOGO
│   │   └── logo.png
│   └── .streamlit/                     # 推荐 - 主题配置
│       └── config.toml
│
└── 📄 其他
    └── requirements.txt                # 可选 - 用于排查问题
```

### 2.2 不需要复制的文件

```
❌ venv/                # 虚拟环境（EXE已内置）
❌ build/               # 打包中间文件
❌ dist/                # 打包输出目录
❌ __pycache__/         # Python缓存（自动生成）
❌ *.log                # 日志文件
❌ OLD/                 # 归档文档
❌ test_*.py            # 测试文件
❌ *.spec               # 打包配置
```

### 2.3 快速打包脚本

在源机器上执行（PowerShell）：

```powershell
# 设置目标路径
$dest = "D:\deploy\charging-agent"
New-Item -ItemType Directory -Force -Path $dest

# 复制必需文件
Copy-Item "charging-agent-launcher.exe" $dest
Copy-Item "*.py" $dest -Exclude "test_*.py","check_*.py","create_*.py","fix_*.py"
Copy-Item "requirements.txt" $dest

# 复制必需目录
$dirs = @("auth","services","pages","core","data","handlers","utils","assets",".streamlit")
foreach ($dir in $dirs) {
    if (Test-Path $dir) {
        Copy-Item $dir $dest -Recurse -Force
    }
}

Write-Host "✅ 部署包已创建: $dest" -ForegroundColor Green
```

---

## 三、部署步骤

### 步骤 1：复制文件

将部署包复制到目标机器，例如：`D:\charging-agent\`

### 步骤 2：运行启动器

双击 `charging-agent-launcher.exe`

### 步骤 3：配置数据库

**首次启动会弹出配置窗口：**

| 配置项 | 说明 | 示例 |
|--------|------|------|
| 数据库主机 | MySQL服务器IP | `192.168.1.100` |
| 端口 | MySQL端口 | `3306` |
| 用户名 | 数据库用户 | `root` |
| 密码 | 数据库密码 | `your_password` |
| 数据库名 | 数据库名称 | `evcipadata` |
| **接入LLM大模型** | 是否启用AI功能 | `☐ 不勾选 = 关闭` |

> 💡 **提示**：如果不使用AI智能助手，可以不勾选"接入LLM大模型"，节省资源。

### 步骤 4：等待启动

配置成功后：
1. 启动器测试数据库连接
2. 自动启动 Streamlit 服务
3. 浏览器自动打开 `http://localhost:8501`

---

## 四、首次登录

### 4.1 默认管理员账号

| 用户名 | 密码 | 角色 |
|--------|------|------|
| `admin` | `Admin@2026` | 管理员 |

> ⚠️ **安全提示**：首次登录后请立即修改默认密码！

### 4.2 角色权限说明

| 角色 | 数据导入 | 数据查询 | 统计分析 | 用户管理 | 审计日志 |
|------|:--------:|:--------:|:--------:|:--------:|:--------:|
| 🔑 管理员 | ✅ | ✅ | ✅ | ✅ | ✅ |
| 📝 操作员 | ❌ | ✅ | ✅ | ❌ | ❌ |
| 👁️ 查看者 | ❌ | ✅ | ❌ | ❌ | ❌ |

### 4.3 创建新用户

1. 以管理员身份登录
2. 点击侧边栏「👥 用户管理」
3. 填写用户信息并选择角色
4. 点击「添加用户」

---

## 五、常见问题

### 5.1 启动问题

| 症状 | 可能原因 | 解决方案 |
|------|----------|----------|
| **闪退/无响应** | 缺少目录 | 检查 `auth/`、`services/`、`pages/` 是否存在 |
| **数据库连接失败** | 配置错误 | 检查IP、端口、用户名、密码 |
| **端口被占用** | 8501已使用 | 启动器会提示是否关闭占用进程 |
| **编码乱码** | 控制台编码 | 正常现象，不影响功能 |

### 5.2 登录问题

| 症状 | 解决方案 |
|------|----------|
| **提示用户名或密码错误** | 使用默认账号 `admin` / `Admin@2026` |
| **登录后页面空白** | 刷新页面或清除浏览器缓存 |
| **忘记密码** | 需要数据库管理员重置 |

### 5.3 功能问题

| 症状 | 解决方案 |
|------|----------|
| **无法导入数据** | 确认使用管理员账号登录 |
| **AI助手无响应** | 检查是否启用了LLM功能，Ollama是否运行 |
| **LOGO不显示** | 检查 `assets/logo.png` 是否存在 |

### 5.4 网络检查

```powershell
# 测试数据库连通性
ping <数据库IP>

# 测试端口连通性
Test-NetConnection -ComputerName <数据库IP> -Port 3306
```

### 5.5 日志文件位置

```
📁 charging-agent/
├── streamlit_launcher.log    # Streamlit 启动日志
└── data_processing.log       # 数据处理日志
```

---

## 六、验证部署成功

### 6.1 检查清单

| 步骤 | 验证方法 | 预期结果 |
|------|----------|----------|
| 1 | 双击EXE | 控制台显示启动信息 |
| 2 | 浏览器打开 | 显示登录页面 |
| 3 | 输入账号密码 | 成功登录，进入主页面 |
| 4 | 切换到「数据管理系统」 | 能看到数据统计 |
| 5 | 查看「用户管理」 | 显示用户列表（管理员） |

### 6.2 功能测试

**数据查询测试：**
1. 进入「📊 数据管理系统」
2. 切换到「👁️ 数据预览」标签
3. 应显示随机抽取的数据

**AI助手测试（如已启用LLM）：**
1. 进入「🤖 AI助手」
2. 输入：`查询充电桩总数`
3. 应返回统计结果

---

## 七、回滚与支持

### 7.1 回滚方案

如果部署失败：
1. 关闭启动器窗口
2. 保留 `*.log` 文件用于问题分析
3. 删除 `config.py` 可重新配置

### 7.2 获取支持

遇到无法解决的问题，请提供：
1. `streamlit_launcher.log` 文件
2. 控制台报错截图
3. 操作系统版本
4. 数据库类型和版本

---

## 附录：完整目录结构

```
charging-agent/                    # 项目根目录
├── charging-agent-launcher.exe    # EXE启动器
├── config.py                      # 配置文件
├── main.py                        # 主入口
├── app.py                         # AI助手
├── data_manager.py                # 数据管理
├── pile_model_query.py            # 充电桩查询
├── agent.py                       # AI智能体
│
├── auth/                          # 认证模块
│   ├── __init__.py
│   ├── authenticator.py           # 认证管理器
│   ├── session_manager.py         # 会话管理
│   ├── password_utils.py          # 密码工具
│   └── permission_checker.py      # 权限检查
│
├── services/                      # 服务层
│   ├── __init__.py
│   ├── user_service.py            # 用户服务
│   └── audit_service.py           # 审计服务
│
├── pages/                         # 管理页面
│   ├── __init__.py
│   ├── user_management.py         # 用户管理
│   └── audit_logs.py              # 审计日志
│
├── core/                          # AI核心模块
├── data/                          # 数据处理模块
├── handlers/                      # 业务处理模块
├── utils/                         # 工具函数
│
├── assets/                        # 静态资源
│   └── logo.png                   # 系统LOGO
│
├── .streamlit/                    # Streamlit配置
│   └── config.toml                # 主题配置
│
└── docs/                          # 文档（可选复制）
```

---

*祝部署顺利！🚀 BY VDBP*
