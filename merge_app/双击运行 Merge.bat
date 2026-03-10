@echo off
chcp 65001 >nul
cd /d "%~dp0"

py -3 -c "exit 0" 2>nul
if errorlevel 1 (
    set PYCMD=python
    echo py -3 not found, using python. If errors occur, run: python -m pip install -r requirements.txt
) else (
    set PYCMD=py -3
)

echo Starting Merge...
%PYCMD% -c "import streamlit" 2>nul
if errorlevel 1 (
    echo Streamlit not found. Installing dependencies...
    %PYCMD% -m pip install -r requirements.txt
    if errorlevel 1 (
        echo Install failed. Run manually: %PYCMD% -m pip install -r requirements.txt
        pause
        exit /b 1
    )
    echo.
)

echo Starting Streamlit in new window, wait a moment...
echo Merge service runs in the window titled "Merge". Do not close that window or the app will stop.
echo.
set STREAMLIT_SERVER_HEADLESS=true
start "Merge" %PYCMD% run_merge.py
timeout /t 8 /nobreak >nul
echo Opening browser: http://localhost:8501
start http://localhost:8501
echo.
echo To exit: close the "Merge" window. This window can be closed too.
echo.
echo If page does not load: check the "Merge" window for errors, or open cmd here and run: %PYCMD% run_merge.py
pause
