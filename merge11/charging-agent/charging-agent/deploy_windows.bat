@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ========================================
REM   充电桩数据管理系统 - Windows 一键部署脚本
REM   适用于离线环境部署
REM ========================================

echo.
echo ========================================
echo   充电桩数据管理系统 - 一键部署脚本
echo   版本: 1.0
echo   适用于: Windows 离线环境
echo ========================================
echo.

REM 设置颜色
color 0A

REM 设置变量
set SCRIPT_DIR=%~dp0
set VENV_DIR=%SCRIPT_DIR%venv
set PYTHON_EXE=
set VENV_ACTIVATE=%VENV_DIR%\Scripts\activate.bat
set REQUIREMENTS=%SCRIPT_DIR%requirements.txt
set WHEELS_DIR=%SCRIPT_DIR%wheels
set CONFIG_FILE=%SCRIPT_DIR%config.py
set UPLOADS_DIR=%SCRIPT_DIR%uploads

REM ========================================
REM   步骤1: 检查Python环境
REM ========================================
echo [1/6] 检查Python环境...
echo.

REM 检查Python是否在PATH中
python --version >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
    echo    ✓ Python已安装: !PYTHON_VERSION!
    set PYTHON_EXE=python
    goto :check_python_version
)

REM 检查python3
python3 --version >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=2" %%i in ('python3 --version 2^>^&1') do set PYTHON_VERSION=%%i
    echo    ✓ Python已安装: !PYTHON_VERSION!
    set PYTHON_EXE=python3
    goto :check_python_version
)

echo    ✗ 错误: 未找到Python环境
echo.
echo    请先安装Python 3.11或更高版本
echo    下载地址: https://www.python.org/downloads/
echo.
pause
exit /b 1

:check_python_version
REM 检查Python版本（需要3.11+）
for /f "tokens=1,2 delims=." %%a in ("!PYTHON_VERSION!") do (
    set MAJOR=%%a
    set MINOR=%%b
)

if !MAJOR! lss 3 (
    echo    ✗ 错误: Python版本过低，需要3.11或更高版本
    pause
    exit /b 1
)

if !MAJOR! equ 3 (
    if !MINOR! lss 11 (
        echo    ✗ 错误: Python版本过低，需要3.11或更高版本，当前版本: !PYTHON_VERSION!
        pause
        exit /b 1
    )
)

echo    ✓ Python版本检查通过
echo.

REM ========================================
REM   步骤2: 检查/创建虚拟环境
REM ========================================
echo [2/6] 检查虚拟环境...
echo.

if exist "%VENV_ACTIVATE%" (
    echo    ✓ 虚拟环境已存在: %VENV_DIR%
    goto :check_dependencies
)

echo    ⚠ 虚拟环境不存在，正在创建...
echo.

REM 创建虚拟环境
%PYTHON_EXE% -m venv "%VENV_DIR%"
if %errorlevel% neq 0 (
    echo    ✗ 错误: 创建虚拟环境失败
    echo    请检查Python安装是否正确
    pause
    exit /b 1
)

echo    ✓ 虚拟环境创建成功
echo.

:check_dependencies
REM ========================================
REM   步骤3: 激活虚拟环境并检查依赖
REM ========================================
echo [3/6] 检查依赖包...
echo.

REM 激活虚拟环境
call "%VENV_ACTIVATE%"
if %errorlevel% neq 0 (
    echo    ✗ 错误: 无法激活虚拟环境
    pause
    exit /b 1
)

REM 升级pip（如果可能）
echo    正在升级pip...
python -m pip install --upgrade pip --quiet --disable-pip-version-check 2>nul

REM 检查关键包是否已安装
echo    检查依赖包安装状态...
python -c "import streamlit" >nul 2>&1
if %errorlevel% equ 0 (
    echo    ✓ 依赖包已安装
    goto :check_config
)

REM 依赖包未安装，尝试安装
echo    ⚠ 检测到依赖包未安装
echo.

REM 检查是否有离线安装包目录
if exist "%WHEELS_DIR%" (
    echo    发现离线安装包目录: %WHEELS_DIR%
    echo    正在从离线包安装依赖...
    echo.
    
    REM 从本地wheel包安装
    pip install --no-index --find-links=%WHEELS_DIR% -r %REQUIREMENTS%
    if %errorlevel% equ 0 (
        echo    ✓ 依赖包安装成功（从离线包）
        goto :check_config
    ) else (
        echo    ✗ 从离线包安装失败
        echo.
    )
)

REM 尝试从requirements.txt安装（如果有网络）
echo    尝试安装依赖包...
pip install -r %REQUIREMENTS% --quiet 2>nul
if %errorlevel% equ 0 (
    echo    ✓ 依赖包安装成功
    goto :check_config
)

REM 安装失败
echo    ✗ 依赖包安装失败
echo.
echo    ========================================
echo    离线环境部署说明:
echo    ========================================
echo.
echo    由于服务器无法联网，请按以下步骤准备离线安装包:
echo.
echo    1. 在有网络的机器上执行以下命令下载wheel包:
echo.
echo       pip download -r requirements.txt -d wheels
echo.
echo    2. 将生成的 wheels 文件夹复制到项目目录
echo.
echo    3. 重新运行此部署脚本
echo.
echo    ========================================
echo.
pause
exit /b 1

:check_config
REM ========================================
REM   步骤4: 检查配置文件
REM ========================================
echo [4/6] 检查配置文件...
echo.

if not exist "%CONFIG_FILE%" (
    echo    ⚠ 配置文件不存在，正在创建...
    echo.
    
    REM 创建默认配置文件
    (
        echo DB_CONFIG = {
        echo     'host': 'localhost',
        echo     'user': 'root',
        echo     'password': '',
        echo     'database': 'evcipadata'
        echo }
    ) > "%CONFIG_FILE%"
    
    echo    ✓ 已创建默认配置文件: %CONFIG_FILE%
    echo.
    echo    ⚠ 请编辑 config.py 文件配置数据库连接信息
    echo.
    
    REM 询问是否现在配置
    set /p CONFIGURE_NOW="    是否现在配置数据库? (Y/N): "
    if /i "!CONFIGURE_NOW!"=="Y" (
        call :configure_database
    )
) else (
    echo    ✓ 配置文件已存在: %CONFIG_FILE%
    
    REM 检查配置是否完整
    python -c "from config import DB_CONFIG; print('OK')" >nul 2>&1
    if %errorlevel% neq 0 (
        echo    ⚠ 配置文件格式可能有误，请检查
    ) else (
        echo    ✓ 配置文件格式正确
    )
)
echo.

REM ========================================
REM   步骤5: 创建必要目录
REM ========================================
echo [5/6] 创建必要目录...
echo.

if not exist "%UPLOADS_DIR%" (
    mkdir "%UPLOADS_DIR%"
    echo    ✓ 已创建上传目录: %UPLOADS_DIR%
) else (
    echo    ✓ 上传目录已存在: %UPLOADS_DIR%
)

REM 创建.streamlit目录（如果不存在）
if not exist "%SCRIPT_DIR%.streamlit" (
    mkdir "%SCRIPT_DIR%.streamlit"
)

REM 创建.streamlit/config.toml（如果不存在）
if not exist "%SCRIPT_DIR%.streamlit\config.toml" (
    (
        echo [server]
        echo maxUploadSize = 600
        echo maxMessageSize = 600
        echo enableXsrfProtection = false
        echo enableCORS = true
    ) > "%SCRIPT_DIR%.streamlit\config.toml"
    echo    ✓ 已创建Streamlit配置文件
) else (
    echo    ✓ Streamlit配置文件已存在
)
echo.

REM ========================================
REM   步骤6: 选择启动应用
REM ========================================
echo [6/6] 准备启动应用...
echo.

:select_app
echo    请选择要启动的应用:
echo.
echo    1. 数据管理系统 (data_manager.py)
echo    2. 充电桩型号查询 (pile_model_query.py)
echo    3. AI助手 (app.py - 需要Ollama)
echo    4. 退出
echo.
set /p APP_CHOICE="    请输入选项 (1-4): "

if "%APP_CHOICE%"=="1" (
    set APP_FILE=data_manager.py
    set APP_NAME=数据管理系统
    goto :start_app
)
if "%APP_CHOICE%"=="2" (
    set APP_FILE=pile_model_query.py
    set APP_NAME=充电桩型号查询
    goto :start_app
)
if "%APP_CHOICE%"=="3" (
    set APP_FILE=app.py
    set APP_NAME=AI助手
    goto :start_app
)
if "%APP_CHOICE%"=="4" (
    echo.
    echo    已取消启动
    pause
    exit /b 0
)

echo    无效选项，请重新选择
echo.
goto :select_app

:start_app
echo.
echo ========================================
echo   正在启动: %APP_NAME%
echo ========================================
echo.
echo    应用地址: http://localhost:8501
echo    按 Ctrl+C 停止应用
echo.
echo ========================================
echo.

REM 启动应用
if "%APP_CHOICE%"=="1" (
    streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600
) else (
    streamlit run %APP_FILE%
)

REM 如果应用退出，暂停以便查看错误信息
if %errorlevel% neq 0 (
    echo.
    echo    ✗ 应用启动失败，错误代码: %errorlevel%
    echo.
    pause
)

exit /b 0

REM ========================================
REM   配置数据库函数
REM ========================================
:configure_database
echo.
echo    数据库配置
echo    ========================================
echo.
set /p DB_HOST="    数据库主机 (默认: localhost): "
if "!DB_HOST!"=="" set DB_HOST=localhost

set /p DB_USER="    数据库用户名 (默认: root): "
if "!DB_USER!"=="" set DB_USER=root

set /p DB_PASSWORD="    数据库密码: "

set /p DB_NAME="    数据库名称 (默认: evcipadata): "
if "!DB_NAME!"=="" set DB_NAME=evcipadata

REM 更新配置文件
(
    echo DB_CONFIG = {
    echo     'host': '!DB_HOST!',
    echo     'user': '!DB_USER!',
    echo     'password': '!DB_PASSWORD!',
    echo     'database': '!DB_NAME!'
    echo }
) > "%CONFIG_FILE%"

echo.
echo    ✓ 数据库配置已保存
echo.
goto :eof

