@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo 正在启动 Merge 多表合并...
echo 若无法打开浏览器，请手动访问: http://localhost:8501
echo 关闭本窗口即可退出程序。
echo.

if exist "Merge多表合并.exe" (
    "Merge多表合并.exe"
) else (
    echo 未找到 Merge多表合并.exe，请将此 bat 放在与 exe 同一目录下。
)

pause
