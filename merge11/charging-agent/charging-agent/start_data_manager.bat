@echo off
REM 启动数据管理系统（支持600MB文件上传）

echo ========================================
echo   启动数据管理系统
echo   支持文件大小: 600MB
echo ========================================
echo.

REM 检查虚拟环境
if not exist "venv\Scripts\activate.bat" (
    echo 错误: 未找到虚拟环境
    echo 请先创建虚拟环境: python -m venv venv
    pause
    exit /b 1
)

REM 激活虚拟环境
call venv\Scripts\activate.bat

REM 启动Streamlit
streamlit run data_manager.py --server.maxUploadSize=600 --server.maxMessageSize=600

pause

