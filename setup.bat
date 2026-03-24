@echo off
echo ============================================
echo  Benzinga Splits - One-Time Setup
echo ============================================
echo.
echo [1/2] Installing Python dependencies...
pip install -r requirements.txt
if errorlevel 1 ( echo ERROR: pip install failed && pause && exit /b 1 )
echo.
echo [2/2] Installing Playwright Chromium browser...
playwright install chromium
if errorlevel 1 ( echo ERROR: playwright install failed && pause && exit /b 1 )
echo.
echo ============================================
echo  Setup complete!
echo  Run refresh.bat to scrape and push data.
echo ============================================
pause
