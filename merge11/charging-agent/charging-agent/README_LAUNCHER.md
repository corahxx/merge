# 充电桩数据管理系统 - 启动器指南

> **版本**: 2.0  
> **更新**: 2026-01-14

---

## 🚀 快速开始

### 使用启动器（用户）

```
1. 双击 charging-agent-launcher.exe
2. 首次使用：配置数据库信息 + LLM开关
3. 登录：admin / Admin@2026
```

### 打包启动器（开发者）

```bash
# 方式一：使用批处理脚本
build_launcher.bat

# 方式二：手动执行
pyinstaller launcher.spec --clean
copy dist\charging-agent-launcher.exe .
```

---

## 📁 部署文件清单

```
charging-agent/
├── 🚀 charging-agent-launcher.exe   # 启动器
├── 📄 config.py                      # 配置文件
├── 📄 main.py                        # 主入口
├── 📄 app.py                         # AI助手
├── 📄 data_manager.py                # 数据管理
├── 📄 pile_model_query.py            # 充电桩查询
├── 📄 agent.py                       # AI智能体
│
├── 📁 auth/                          # 认证模块 ⭐必需
├── 📁 services/                      # 服务层 ⭐必需
├── 📁 pages/                         # 管理页面 ⭐必需
├── 📁 core/                          # AI核心 ⭐必需
├── 📁 data/                          # 数据处理 ⭐必需
├── 📁 handlers/                      # 业务处理 ⭐必需
├── 📁 utils/                         # 工具函数 ⭐必需
│
├── 📁 assets/                        # 静态资源（LOGO）
└── 📁 .streamlit/                    # 主题配置
```

---

## ⚙️ 启动器功能

### 配置窗口

首次启动或配置丢失时，显示配置窗口：

| 配置项 | 说明 |
|--------|------|
| 数据库主机 | MySQL服务器地址 |
| 端口 | 默认3306 |
| 用户名/密码 | 数据库认证信息 |
| 数据库名 | 默认evcipadata |
| **接入LLM大模型** | 是否启用AI功能（勾选=启用） |

### 端口冲突处理

如果8501端口被占用，启动器会提示：
- `Y` - 关闭占用进程并继续
- `N` - 取消启动
- `S` - 跳过检查强制启动

### 日志文件

```
streamlit_launcher.log  # 启动日志
data_processing.log     # 数据处理日志
```

---

## 🔧 开发说明

### 修改launcher.spec添加新依赖

```python
# launcher.spec
hiddenimports=streamlit_hiddenimports + [
    # ... 现有依赖 ...
    'your_new_module',  # 添加新依赖
],
```

### 重新打包

```bash
pyinstaller launcher.spec --clean
copy dist\charging-agent-launcher.exe .
```

---

## 📖 相关文档

- [EXE快速部署指南](docs/EXE快速部署指南.md) - 完整部署说明
- [启动器故障排除](docs/启动器故障排除.md) - 问题排查
- [账号管理体系](docs/账号管理体系可行性分析.md) - 用户权限说明

---

*🚀 BY VDBP | 本地私有化部署*
