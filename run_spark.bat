@echo off
:: Use the folder this script lives in
set PROJECT_ROOT=%~dp0
if "%PROJECT_ROOT:~-1%"=="\" set PROJECT_ROOT=%PROJECT_ROOT:~0,-1%
cd /d "%PROJECT_ROOT%"

set HADOOP_HOME=C:\hadoop
set OUTPUT_BASE=%PROJECT_ROOT%\output
set CHECKPOINT_BASE=%PROJECT_ROOT%\checkpoints
set KAFKA_BOOTSTRAP=localhost:9092
set PATH=C:\hadoop\bin;%PATH%

echo PROJECT_ROOT = %PROJECT_ROOT%
echo HADOOP_HOME  = %HADOOP_HOME%
echo OUTPUT_BASE  = %OUTPUT_BASE%

if exist "C:\hadoop\bin\winutils.exe" (
    echo winutils.exe FOUND
) else (
    echo ERROR: winutils.exe NOT found at C:\hadoop\bin\winutils.exe
    pause
    exit /b 1
)

echo.
echo Starting Spark stream processor...
echo.

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --driver-memory 1g --conf spark.sql.shuffle.partitions=4 spark\stream_processor.py

pause