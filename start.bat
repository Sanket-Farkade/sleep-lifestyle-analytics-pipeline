@echo off
setlocal EnableDelayedExpansion

title Sleep Pipeline Launcher
color 0A

echo.
echo  ================================================
echo   Sleep ^& Lifestyle Analytics Pipeline
echo  ================================================
echo.

set PROJECT_ROOT=D:\sleep-pipeline
set OUTPUT_BASE=D:\sleep-pipeline\output
set CHECKPOINT_BASE=D:\sleep-pipeline\checkpoints
set KAFKA_BOOTSTRAP=localhost:9092
set HADOOP_HOME=C:\hadoop
set PYSPARK_BIN=C:\Users\ayush\AppData\Local\Python\pythoncore-3.14-64\Lib\site-packages\pyspark\bin

echo   Project : %PROJECT_ROOT%
echo   Hadoop  : %HADOOP_HOME%
echo   Spark   : %PYSPARK_BIN%
echo.

:: ── STEP 1: Kafka ─────────────────────────────────────────────
echo [1/5] Starting Kafka + Zookeeper ...
cd /d %PROJECT_ROOT%
docker compose up -d
echo         Waiting 30s ...
timeout /t 30 /nobreak

:: ── STEP 2: Topics ────────────────────────────────────────────
echo.
echo [2/5] Creating topics ...
docker exec sleep-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic sleep-lifestyle --partitions 3 --replication-factor 1
docker exec sleep-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic personal-info  --partitions 3 --replication-factor 1
docker exec sleep-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic profession      --partitions 3 --replication-factor 1
echo         Topics ready.

:: ── STEP 3: Producers ─────────────────────────────────────────
echo.
echo [3/5] Starting producers ...
start "Lifestyle Producer"  cmd /k "cd /d D:\sleep-pipeline && set KAFKA_BOOTSTRAP=localhost:9092 && python producers\lifestyle_producer.py"
timeout /t 2 /nobreak >nul
start "Personal Producer"   cmd /k "cd /d D:\sleep-pipeline && set KAFKA_BOOTSTRAP=localhost:9092 && python producers\personal_producer.py"
timeout /t 2 /nobreak >nul
start "Profession Producer" cmd /k "cd /d D:\sleep-pipeline && set KAFKA_BOOTSTRAP=localhost:9092 && python producers\profession_producer.py"
echo         Producers started.

:: ── STEP 4: Spark ─────────────────────────────────────────────
echo.
echo [4/5] Starting Spark ...
start "Spark Processor" cmd /k "cd /d D:\sleep-pipeline && set HADOOP_HOME=C:\hadoop && set OUTPUT_BASE=D:\sleep-pipeline\output && set CHECKPOINT_BASE=D:\sleep-pipeline\checkpoints && set KAFKA_BOOTSTRAP=localhost:9092 && set PATH=C:\hadoop\bin;C:\Users\ayush\AppData\Local\Python\pythoncore-3.14-64\Lib\site-packages\pyspark\bin;%PATH% && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --driver-memory 1g --conf spark.sql.shuffle.partitions=4 spark\stream_processor.py"

:: ── STEP 5: Dashboard ─────────────────────────────────────────
echo [5/5] Waiting 45s then opening dashboard ...
timeout /t 45 /nobreak
start "Dash Dashboard" cmd /k "cd /d D:\sleep-pipeline && set OUTPUT_BASE=D:\sleep-pipeline\output && python dashboard.py"

echo.
echo  ================================================
echo   All done! Dashboard: http://localhost:8050
echo   To stop: run stop.bat
echo  ================================================
echo.
pause