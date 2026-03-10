@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ========================================
REM   准备离线安装包脚本
REM   在有网络的机器上运行此脚本
REM ========================================

echo.
echo ========================================
echo   准备离线安装包
echo   用于离线环境部署
echo ========================================
echo.

set SCRIPT_DIR=%~dp0
set WHEELS_DIR=%SCRIPT_DIR%wheels
set REQUIREMENTS=%SCRIPT_DIR%requirements.txt

REM 检查requirements.txt是否存在
if not exist "%REQUIREMENTS%" (
    echo    ✗ 错误: 未找到 requirements.txt 文件
    pause
    exit /b 1
)

REM 检查Python环境
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo    ✗ 错误: 未找到Python环境
    pause
    exit /b 1
)

echo    ✓ Python环境检查通过
echo.

REM 创建wheels目录
if not exist "%WHEELS_DIR%" (
    mkdir "%WHEELS_DIR%"
    echo    ✓ 已创建目录: %WHEELS_DIR%
) else (
    echo    ⚠ 目录已存在: %WHEELS_DIR%
    set /p OVERWRITE="    是否清空并重新下载? (Y/N): "
    if /i "!OVERWRITE!"=="Y" (
        rmdir /s /q "%WHEELS_DIR%"
        mkdir "%WHEELS_DIR%"
        echo    ✓ 目录已清空
    )
)
echo.

REM 升级pip
echo    正在升级pip...
python -m pip install --upgrade pip --quiet

echo.
echo    正在下载依赖包...
echo    这可能需要几分钟时间，请耐心等待...
echo.

REM 下载wheel包
pip download -r "%REQUIREMENTS%" -d "%WHEELS_DIR%"

if %errorlevel% equ 0 (
    echo.
    echo    ========================================
    echo    ✓ 离线安装包准备完成！
    echo    ========================================
    echo.
    echo    下载位置: %WHEELS_DIR%
    echo.
    
    REM 统计文件数量
    for /f %%i in ('dir /b "%WHEELS_DIR%\*.whl" 2^>nul ^| find /c /v ""') do set FILE_COUNT=%%i
    echo    已下载文件数: !FILE_COUNT!
    echo.
    
    echo    下一步操作:
    echo    1. 将整个项目文件夹（包括wheels目录）复制到目标服务器
    echo    2. 在目标服务器上运行 deploy_windows.bat
    echo.
) else (
    echo.
    echo    ✗ 下载失败，请检查网络连接
    echo.
    pause
    exit /b 1
)

pause

