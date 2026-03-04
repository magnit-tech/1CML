@echo off
echo Настройка планировщика для анализа блокировок

:: Каждый час (на 5-й минуте)
schtasks /create /tn "1CML Check Deadlocks" /tr "C:\Python39\python.exe C:\1CML\scripts\check_deadlocks.py" /sc hourly /st 00:05 /f

:: Каждый день в 08:00 - отчет по трендам
schtasks /create /tn "1CML Deadlock Report" /tr "C:\Python39\python.exe C:\1CML\scripts\analyze_lock_trends.py" /sc daily /st 08:00 /f

echo Готово!
pause
