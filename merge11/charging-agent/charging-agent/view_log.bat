@echo off
chcp 65001 >nul
REM 查看Streamlit启动日志

echo ========================================
echo   查看Streamlit启动日志
echo ========================================
echo.

set LOG_FILE=app\streamlit_launcher.log

if exist "%LOG_FILE%" (
    echo 找到日志文件: %LOG_FILE%
    echo.
    echo ========================================
    echo 日志内容:
    echo ========================================
    echo.
    type "%LOG_FILE%"
    echo.
    echo ========================================
) else (
    echo 未找到日志文件: %LOG_FILE%
    echo.
    echo 可能的原因:
    echo   1. 尚未运行过启动器
    echo   2. 日志文件创建失败
    echo   3. 日志文件在其他位置
    echo.
    echo 请检查以下位置:
    echo   - app\streamlit_launcher.log
    echo   - streamlit_launcher.log (根目录)
    echo.
)

pause
