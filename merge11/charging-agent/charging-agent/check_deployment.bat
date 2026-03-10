@echo off
chcp 65001 >nul
REM 检查部署结构是否正确

echo ========================================
echo   部署结构检查
echo ========================================
echo.

set EXE_NAME=charging-agent-launcher.exe
set APP_DIR=app

echo [1/4] 检查EXE文件...
if exist "%EXE_NAME%" (
    echo    ✅ %EXE_NAME% 存在
) else (
    echo    ❌ %EXE_NAME% 不存在
    echo    请确保EXE文件在当前目录
    pause
    exit /b 1
)
echo.

echo [2/4] 检查源代码目录...
if exist "%APP_DIR%" (
    echo    ✅ %APP_DIR% 目录存在
) else (
    echo    ⚠️  %APP_DIR% 目录不存在
    echo    检查根目录是否有源代码文件...
    if exist "main.py" (
        echo    ✅ 找到 main.py，将使用根目录作为源代码目录
    ) else (
        echo    ❌ 未找到源代码文件
        echo    请确保源代码在 %APP_DIR%\ 目录下，或与EXE同级
        pause
        exit /b 1
    )
)
echo.

echo [3/4] 检查关键文件...
set FOUND=0
if exist "%APP_DIR%\main.py" (
    echo    ✅ %APP_DIR%\main.py
    set FOUND=1
) else if exist "main.py" (
    echo    ✅ main.py (根目录)
    set FOUND=1
)

if exist "%APP_DIR%\data_manager.py" (
    echo    ✅ %APP_DIR%\data_manager.py
    set FOUND=1
) else if exist "data_manager.py" (
    echo    ✅ data_manager.py (根目录)
    set FOUND=1
)

if %FOUND%==0 (
    echo    ❌ 未找到 main.py 或 data_manager.py
    pause
    exit /b 1
)
echo.

echo [4/4] 检查配置文件...
if exist "%APP_DIR%\config.py" (
    echo    ✅ %APP_DIR%\config.py 存在
) else if exist "config.py" (
    echo    ✅ config.py (根目录) 存在
) else (
    echo    ⚠️  配置文件不存在（首次运行时会自动创建）
)
echo.

echo ========================================
echo ✅ 部署结构检查完成
echo ========================================
echo.
echo 💡 部署结构应该是:
echo    charging-agent-launcher.exe
echo    app\
echo      ├── main.py
echo      ├── config.py
echo      ├── core\
echo      ├── data\
echo      └── ...
echo.
echo    或者:
echo    charging-agent-launcher.exe
echo    main.py
echo    config.py
echo    core\
echo    data\
echo    ...
echo.

pause
