# Conda 环境管理教程

本教程将指导您如何使用 Conda 来管理充电桩数据管理系统项目的 Python 环境。

## 📋 目录

1. [Conda 简介](#conda-简介)
2. [安装 Conda](#安装-conda)
3. [创建项目环境](#创建项目环境)
4. [安装依赖包](#安装依赖包)
5. [环境管理](#环境管理)
6. [运行项目](#运行项目)
7. [常见问题](#常见问题)

---

## Conda 简介

Conda 是一个开源的包管理和环境管理系统，可以：
- 创建独立的 Python 环境
- 管理不同版本的 Python 和包
- 避免包冲突
- 方便环境迁移和分享

### Conda vs venv

| 特性 | Conda | venv |
|------|-------|------|
| Python 版本管理 | ✅ 支持 | ❌ 不支持 |
| 非 Python 包 | ✅ 支持 | ❌ 不支持 |
| 跨平台 | ✅ 优秀 | ✅ 良好 |
| 包管理 | conda/pip | pip only |
| 环境导出 | ✅ conda env export | ✅ pip freeze |

---

## 安装 Conda

### 方式1：安装 Anaconda（推荐新手）

Anaconda 包含 Conda 和大量预装的科学计算包。

**Windows:**
1. 访问 [Anaconda 官网](https://www.anaconda.com/products/distribution)
2. 下载 Windows 安装程序
3. 运行安装程序，按提示安装
4. 安装时勾选 "Add Anaconda to PATH"（可选，但推荐）

**Linux/macOS:**
```bash
# 下载安装脚本
wget https://repo.anaconda.com/archive/Anaconda3-2024.02-1-Linux-x86_64.sh

# 运行安装脚本
bash Anaconda3-2024.02-1-Linux-x86_64.sh

# 按照提示完成安装
# 安装完成后重启终端或运行：
source ~/.bashrc
```

### 方式2：安装 Miniconda（推荐，更轻量）

Miniconda 只包含 Conda 和 Python，不包含预装包。

**Windows:**
1. 访问 [Miniconda 官网](https://docs.conda.io/en/latest/miniconda.html)
2. 下载 Windows 安装程序
3. 运行安装程序

**Linux/macOS:**
```bash
# 下载安装脚本
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# 运行安装脚本
bash Miniconda3-latest-Linux-x86_64.sh

# 按照提示完成安装
source ~/.bashrc
```

### 验证安装

打开终端（Windows: Anaconda Prompt 或 PowerShell），运行：

```bash
conda --version
```

如果显示版本号（如 `conda 23.11.0`），说明安装成功。

---

## 创建项目环境

### 1. 进入项目目录

```bash
cd /path/to/charging-agent
```

### 2. 创建 Conda 环境

**方式1：指定 Python 版本创建（推荐）**

```bash
# 创建名为 charging-agent 的环境，Python 版本 3.11
conda create -n charging-agent python=3.11 -y
```

**方式2：从 environment.yml 创建（如果存在）**

```bash
# 如果项目有 environment.yml 文件
conda env create -f environment.yml
```

**方式3：创建并指定安装路径**

```bash
# 在项目目录下创建环境（环境位于项目目录）
conda create --prefix ./conda-env python=3.11 -y
```

### 3. 激活环境

**Windows:**
```bash
conda activate charging-agent
```

**Linux/macOS:**
```bash
conda activate charging-agent
```

激活成功后，命令行提示符前会显示 `(charging-agent)`。

### 4. 验证环境

```bash
# 查看当前 Python 版本
python --version

# 查看当前环境路径
conda info --envs

# 查看已安装的包
conda list
```

---

## 安装依赖包

### 方式1：使用 pip 安装（推荐）

由于项目使用 `requirements.txt`，推荐使用 pip 安装：

```bash
# 确保已激活环境
conda activate charging-agent

# 升级 pip
pip install --upgrade pip

# 安装项目依赖
pip install -r requirements.txt
```

### 方式2：使用 conda 安装（部分包）

某些包可以通过 conda 安装，通常更稳定：

```bash
# 安装基础包
conda install pandas numpy -y

# 安装其他包（如果 conda 仓库有）
conda install streamlit sqlalchemy pymysql -y

# 剩余包用 pip 安装
pip install -r requirements.txt
```

### 方式3：混合安装（最佳实践）

```bash
# 1. 先用 conda 安装基础科学计算包
conda install pandas numpy -y

# 2. 再用 pip 安装项目特定依赖
pip install -r requirements.txt
```

### 验证安装

```bash
# 检查关键包是否安装成功
python -c "import streamlit; print('Streamlit:', streamlit.__version__)"
python -c "import pandas; print('Pandas:', pandas.__version__)"
python -c "import pymysql; print('PyMySQL installed')"
```

---

## 环境管理

### 查看所有环境

```bash
conda env list
# 或
conda info --envs
```

### 激活/停用环境

```bash
# 激活环境
conda activate charging-agent

# 停用环境
conda deactivate
```

### 更新包

```bash
# 更新 conda 本身
conda update conda

# 更新所有包
conda update --all

# 更新特定包
conda update pandas

# 使用 pip 更新
pip install --upgrade streamlit
```

### 导出环境配置

**导出为 environment.yml（推荐）**

```bash
# 激活环境后
conda env export > environment.yml

# 只导出明确安装的包（不包括依赖）
conda env export --from-history > environment.yml
```

**导出为 requirements.txt（pip 格式）**

```bash
pip freeze > requirements.txt
```

### 删除环境

```bash
# 先停用环境
conda deactivate

# 删除环境
conda env remove -n charging-agent
```

### 克隆环境

```bash
# 克隆现有环境
conda create -n charging-agent-backup --clone charging-agent
```

### 清理缓存

```bash
# 清理未使用的包和缓存
conda clean --all
```

---

## 运行项目

### 1. 激活环境

```bash
conda activate charging-agent
```

### 2. 配置数据库

编辑 `config.py` 文件，设置数据库连接信息：

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'evcipadata'
}
```

### 3. 启动应用

**启动数据管理系统：**

```bash
streamlit run data_manager.py
```

**启动充电桩型号查询：**

```bash
streamlit run pile_model_query.py
```

**启动 AI 助手（需要 Ollama）：**

```bash
streamlit run app.py
```

### 4. 访问应用

浏览器会自动打开，或手动访问：
- 数据管理系统：http://localhost:8501
- 充电桩型号查询：http://localhost:8501
- AI 助手：http://localhost:8501

---

## 创建启动脚本

### Windows 启动脚本（start_conda.bat）

创建 `start_conda.bat` 文件：

```batch
@echo off
echo ========================================
echo   启动充电桩数据管理系统 (Conda)
echo ========================================
echo.

REM 激活 Conda 环境
call conda activate charging-agent

REM 检查环境是否激活成功
if errorlevel 1 (
    echo 错误: 无法激活 Conda 环境
    echo 请先创建环境: conda create -n charging-agent python=3.11
    pause
    exit /b 1
)

REM 启动 Streamlit
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600

pause
```

### Linux/macOS 启动脚本（start_conda.sh）

创建 `start_conda.sh` 文件：

```bash
#!/bin/bash

echo "========================================"
echo "  启动充电桩数据管理系统 (Conda)"
echo "========================================"
echo ""

# 初始化 Conda（如果未初始化）
eval "$(conda shell.bash hook)"

# 激活环境
conda activate charging-agent

# 检查环境是否激活成功
if [ $? -ne 0 ]; then
    echo "错误: 无法激活 Conda 环境"
    echo "请先创建环境: conda create -n charging-agent python=3.11"
    exit 1
fi

# 启动 Streamlit
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600
```

给脚本添加执行权限：

```bash
chmod +x start_conda.sh
```

---

## 创建 environment.yml

为了便于环境分享和部署，建议创建 `environment.yml` 文件：

```yaml
name: charging-agent
channels:
  - defaults
  - conda-forge
dependencies:
  - python=3.11
  - pip
  - pandas=2.1.4
  - pip:
    - streamlit==1.32.0
    - langchain-community==0.2.10
    - langchain-core==0.2.23
    - ollama==0.1.4
    - PyMySQL==1.1.0
    - openpyxl==3.1.2
    - xlrd==2.0.1
    - sqlalchemy==2.0.23
    - plotly==5.18.0
    - reportlab==4.0.7
```

**使用方法：**

```bash
# 从 environment.yml 创建环境
conda env create -f environment.yml

# 更新环境（如果 environment.yml 有变化）
conda env update -f environment.yml --prune
```

---

## 常见问题

### 1. Conda 命令未找到

**问题：** 运行 `conda` 命令提示 "command not found"

**解决方案：**

**Windows:**
- 使用 "Anaconda Prompt" 而不是普通 CMD
- 或将 Conda 添加到系统 PATH

**Linux/macOS:**
```bash
# 初始化 Conda
conda init bash  # 或 conda init zsh

# 重启终端或运行
source ~/.bashrc
```

### 2. 环境激活失败

**问题：** `conda activate` 提示错误

**解决方案：**

```bash
# 使用 conda activate（新版本）
conda activate charging-agent

# 或使用 source activate（旧版本）
source activate charging-agent

# 检查环境是否存在
conda env list
```

### 3. 包安装失败

**问题：** 使用 `conda install` 安装某些包失败

**解决方案：**

```bash
# 尝试从 conda-forge 频道安装
conda install -c conda-forge package_name

# 或使用 pip 安装
pip install package_name

# 检查包是否在 conda 仓库中
conda search package_name
```

### 4. 环境占用空间过大

**问题：** Conda 环境占用磁盘空间过多

**解决方案：**

```bash
# 清理未使用的包
conda clean --all

# 删除不需要的环境
conda env remove -n old-environment

# 使用 pip 而不是 conda 安装包（更轻量）
pip install package_name
```

### 5. 多个 Python 版本冲突

**问题：** 系统有多个 Python 版本，导致混乱

**解决方案：**

```bash
# 使用 Conda 管理 Python 版本
conda create -n charging-agent python=3.11

# 在环境中使用特定 Python
conda activate charging-agent
python --version  # 应该显示 3.11.x
```

### 6. 环境无法导出

**问题：** `conda env export` 失败

**解决方案：**

```bash
# 确保已激活环境
conda activate charging-agent

# 导出环境
conda env export > environment.yml

# 如果失败，尝试导出为 requirements.txt
pip freeze > requirements.txt
```

### 7. 跨平台环境问题

**问题：** 在 Windows 创建的环境无法在 Linux 使用

**解决方案：**

```bash
# 导出时不包含平台特定信息
conda env export --no-builds > environment.yml

# 或使用 --from-history 只导出明确安装的包
conda env export --from-history > environment.yml
```

---

## Conda 常用命令速查

### 环境管理

```bash
# 创建环境
conda create -n env_name python=3.11

# 激活环境
conda activate env_name

# 停用环境
conda deactivate

# 删除环境
conda env remove -n env_name

# 列出所有环境
conda env list

# 克隆环境
conda create -n new_env --clone old_env
```

### 包管理

```bash
# 安装包
conda install package_name
pip install package_name

# 更新包
conda update package_name
pip install --upgrade package_name

# 卸载包
conda remove package_name
pip uninstall package_name

# 列出已安装包
conda list
pip list

# 搜索包
conda search package_name
```

### 环境导出/导入

```bash
# 导出环境
conda env export > environment.yml
conda env export --from-history > environment.yml

# 创建环境
conda env create -f environment.yml

# 更新环境
conda env update -f environment.yml --prune
```

### 信息查询

```bash
# Conda 版本
conda --version

# Python 版本
python --version

# 环境信息
conda info
conda info --envs

# 包信息
conda list package_name
```

### 清理

```bash
# 清理所有缓存
conda clean --all

# 清理索引缓存
conda clean --index-cache

# 清理包缓存
conda clean --packages
```

---

## 最佳实践

### 1. 环境命名规范

- 使用项目名称：`charging-agent`
- 使用版本号：`charging-agent-v1`
- 使用用途：`charging-agent-dev`, `charging-agent-prod`

### 2. 依赖管理

- 使用 `environment.yml` 管理 conda 包
- 使用 `requirements.txt` 管理 pip 包
- 定期更新依赖版本
- 锁定版本号以确保一致性

### 3. 环境隔离

- 每个项目使用独立环境
- 不要在生产环境使用开发环境
- 定期备份环境配置

### 4. 性能优化

- 使用 conda-forge 频道（包更新更快）
- 定期清理缓存：`conda clean --all`
- 避免安装不必要的包

### 5. 团队协作

- 提交 `environment.yml` 到版本控制
- 使用 `--from-history` 导出，避免平台差异
- 在 README 中说明环境要求

---

## 总结

使用 Conda 管理项目环境的优势：

✅ **版本控制**：轻松管理 Python 和包版本  
✅ **环境隔离**：避免包冲突  
✅ **跨平台**：Windows/Linux/macOS 通用  
✅ **易于分享**：通过 environment.yml 分享环境  
✅ **包管理**：支持 conda 和 pip 两种方式  

通过本教程，您应该能够：
- 安装和配置 Conda
- 创建和管理项目环境
- 安装项目依赖
- 运行项目应用
- 解决常见问题

如有问题，请参考 [Conda 官方文档](https://docs.conda.io/) 或项目 README。

---

**最后更新**: 2025年

