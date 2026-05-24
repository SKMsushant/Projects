@echo off
setlocal enabledelayedexpansion

echo ==========================================================
echo ⚡ QUANT ENGINE - AUTOMATED PORTABLE LAUNCHER ⚡
echo ==========================================================
echo Detecting Python environment...

:: 1. Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Error: Python is not installed or not added to your system PATH!
    echo Please install Python 3.9 - 3.11 from python.org and try again.
    pause
    exit /b 1
)

:: 2. Resolve virtual environment directory in the current app folder
set VENV_DIR=%~dp0venv

if not exist "%VENV_DIR%" (
    echo ⏳ No virtual environment detected. Creating one now at .\venv...
    python -m venv "%VENV_DIR%"
    if !errorlevel! neq 0 (
        echo ❌ Failed to create virtual environment!
        pause
        exit /b 1
    )
    echo  Virtual environment created successfully.
)

:: 3. Activate Virtual Environment
echo 🔌 Activating virtual environment...
call "%VENV_DIR%\Scripts\activate.bat"

:: 4. Install Dependencies
echo ⏳ Verifying and installing dependencies from requirements.txt...
python -m pip install --upgrade pip
pip install -r "%~dp0requirements.txt"
if %errorlevel% neq 0 (
    echo ❌ Error occurred during dependency installation!
    pause
    exit /b 1
)
echo  All libraries are fully synchronized and up to date!

:: 5. Launch App
echo 🚀 Launching Quant Engine Streamlit Dashboard...
streamlit run "%~dp0main_app.py"

pause
