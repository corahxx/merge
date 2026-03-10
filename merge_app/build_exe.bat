@echo off
cd /d "%~dp0"

echo Using Python 3 to build. Checking dependencies...
py -3 -c "import streamlit, pandas, openpyxl" 2>nul
if errorlevel 1 (
    echo.
    echo streamlit/pandas/openpyxl not found. Install first:
    echo   py -3 -m pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

echo Checking PyInstaller...
py -3 -m pyinstaller --version >nul 2>&1
if errorlevel 1 (
    echo PyInstaller not found. Installing...
    py -3 -m pip install pyinstaller
    py -3 -m pyinstaller --version >nul 2>&1
    if errorlevel 1 (
        echo Still missing. In this folder open cmd and run: py -3 -m pip install pyinstaller
        pause
        exit /b 1
    )
)

if exist "dist\Merge多表合并.exe" (
    echo.
    echo If you see "Access denied", close the running Merge exe and run this script again.
    echo.
)

echo.
echo Building Merge (--clean)...
py -3 -m pyinstaller --clean merge_exe.spec

if errorlevel 1 (
    echo.
    echo Build failed. Check errors above.
    echo If "Access denied", close the exe in dist and retry.
    pause
    exit /b 1
)

if exist "启动_Merge多表合并.bat" (
    copy /y "启动_Merge多表合并.bat" "dist\"
    echo Copied launcher bat to dist.
)

echo.
echo Build done. Output: dist\Merge多表合并.exe
echo If exe does not start, run the launcher bat in dist to see errors.
pause
