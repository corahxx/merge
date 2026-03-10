@echo off
chcp 65001 >nul
REM 充电桩数据管理系统 - 启动器打包脚本

echo ========================================
echo   充电桩数据管理系统 - 启动器打包
echo ========================================
echo.

REM 检查PyInstaller是否安装
python -c "import PyInstaller" 2>nul
if %errorlevel% neq 0 (
    echo [1/3] 安装PyInstaller...
    pip install pyinstaller --quiet --disable-pip-version-check
    REM 再次检查是否安装成功
    python -c "import PyInstaller" 2>nul
    if %errorlevel% neq 0 (
        echo ❌ PyInstaller安装失败，请手动安装: pip install pyinstaller
        pause
        exit /b 1
    )
    echo ✅ PyInstaller安装成功
    echo.
) else (
    echo [1/3] PyInstaller已安装
    echo.
)

echo [2/3] 清理旧的打包文件...
if exist "build" rmdir /s /q "build"
if exist "dist\charging-agent-launcher" rmdir /s /q "dist\charging-agent-launcher"
if exist "dist\charging-agent-launcher.exe" del /q "dist\charging-agent-launcher.exe"
echo ✅ 清理完成
echo.

echo [3/3] 开始打包...
echo.

REM 检查spec文件是否存在
if not exist "launcher.spec" (
    echo ❌ 错误: 未找到 launcher.spec 文件
    echo    请确保 launcher.spec 文件在当前目录
    pause
    exit /b 1
)

pyinstaller launcher.spec

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo ✅ 打包成功！
    echo ========================================
    echo.
    echo 📦 打包文件位置: dist\charging-agent-launcher.exe
    echo.
    echo 💡 使用说明:
    echo    1. 将 dist\charging-agent-launcher.exe 复制到项目根目录
    echo    2. 确保项目源代码在 app\ 目录下（或与EXE同级）
    echo    3. 双击 charging-agent-launcher.exe 启动
    echo.
) else (
    echo.
    echo ❌ 打包失败，请检查错误信息
    echo.
)

pause
