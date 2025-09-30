@echo off
setlocal EnableExtensions
REM 用 UTF-8 避免中文参数乱码
chcp 65001 >nul

REM 切到项目根目录
cd /d "D:\WeeklyReport"

REM 1) 优先使用项目自带 Python
set "PY_EXE=D:\WeeklyReport\py\python.exe"
if exist "%PY_EXE%" goto :PY_OK
set "PY_EXE=D:\WeeklyReport\venv\Scripts\python.exe"
if exist "%PY_EXE%" goto :PY_OK

REM 2) 其次Python 3.13（
set "PY_EXE=C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe"
if exist "%PY_EXE%" goto :PY_OK

REM 3) 最后用 PATH 里的
for /f "delims=" %%P in ('where python 2^>nul') do (set "PY_EXE=%%P" & goto :PY_OK)

echo [ERROR] 找不到 python.exe，请安装或把路径写到上面的 PY_EXE。
pause
exit /b 9009

:PY_OK
echo Using Python: "%PY_EXE%"
if not exist "logs" mkdir "logs"

REM ===== 绝对路径，全部写成一行，纯 ASCII 引号 =====
"%PY_EXE%" "D:\WeeklyReport\email_sender\send_weekly.py" --md "D:\WeeklyReport\docs\2025-W39-bold.md" --subject "行业周报 · 2025 第39周" --recipients "D:\WeeklyReport\email_sender\recipients.csv" --css "D:\WeeklyReport\email_sender\email_style.css" --send  >> "D:\WeeklyReport\logs\weekly-2025-W39.log" 2>&1

set ERR=%ERRORLEVEL%
echo ExitCode=%ERR%  日志: D:\WeeklyReport\logs\weekly-2025-W39.log
pause
exit /b %ERR%
