@echo off
cd /d D:\sleep-pipeline

set HADOOP_HOME=C:\hadoop
set HADOOP_BIN=C:\hadoop\bin
set OUTPUT_BASE=D:\sleep-pipeline\output
set CHECKPOINT_BASE=D:\sleep-pipeline\checkpoints
set KAFKA_BOOTSTRAP=localhost:9092
set PYSPARK_PYTHON=C:\Users\ayush\AppData\Local\Python\pythoncore-3.14-64\python.exe

set PATH=%HADOOP_BIN%;%PATH%

echo HADOOP_HOME = %HADOOP_HOME%
echo HADOOP_BIN  = %HADOOP_BIN%
echo Verifying winutils...
if exist "%HADOOP_BIN%\winutils.exe" (
    echo winutils.exe FOUND
) else (
    echo ERROR: winutils.exe NOT found at %HADOOP_BIN%\winutils.exe
    pause
    exit /b 1
)

echo.
echo Starting Spark stream processor...
echo.

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --driver-memory 1g --conf spark.sql.shuffle.partitions=4 spark\stream_processor.py

pause
