@echo off
setlocal EnableDelayedExpansion

title Workforce Wellbeing Pipeline - Launcher
color 0A

echo.
echo  ================================================
echo   Workforce Wellbeing Analytics Pipeline
echo  ================================================
echo.
echo   This starts: Kafka + Topics + Producers + Dashboard
echo   Run run_spark.bat SEPARATELY for the Spark job.
echo.

:: Use the folder this script lives in - no hardcoded path
set PROJECT_ROOT=%~dp0
if "%PROJECT_ROOT:~-1%"=="\" set PROJECT_ROOT=%PROJECT_ROOT:~0,-1%
set OUTPUT_BASE=%PROJECT_ROOT%\output
set KAFKA_BOOTSTRAP=localhost:9092

echo   Project : %PROJECT_ROOT%
echo   Output  : %OUTPUT_BASE%
echo.

:: -- STEP 0: Clean up any leftover containers --
echo [0/4] Removing any leftover containers ...
cd /d "%PROJECT_ROOT%"
docker compose down >nul 2>&1
docker rm -f sleep-kafka sleep-zookeeper >nul 2>&1
echo         Clean.

:: -- STEP 1: Kafka --
echo.
echo [1/4] Starting Kafka + Zookeeper ...
docker compose up -d
echo         Waiting 30s ...
timeout /t 30 /nobreak

:: -- STEP 2: Topics --
echo.
echo [2/4] Creating topics ...
docker exec sleep-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic sleep-lifestyle --partitions 3 --replication-factor 1
docker exec sleep-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic personal-info  --partitions 3 --replication-factor 1
docker exec sleep-kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic profession      --partitions 3 --replication-factor 1
echo         Topics ready.

:: -- STEP 3: Producers --
echo.
echo [3/4] Starting producers (line-by-line logging) ...
start "Lifestyle Producer"  cmd /k "cd /d "%PROJECT_ROOT%" && set KAFKA_BOOTSTRAP=localhost:9092 && python producers\lifestyle_producer.py"
timeout /t 2 /nobreak >nul
start "Personal Producer"   cmd /k "cd /d "%PROJECT_ROOT%" && set KAFKA_BOOTSTRAP=localhost:9092 && python producers\personal_producer.py"
timeout /t 2 /nobreak >nul
start "Profession Producer" cmd /k "cd /d "%PROJECT_ROOT%" && set KAFKA_BOOTSTRAP=localhost:9092 && python producers\profession_producer.py"
echo         Producers started.

:: -- STEP 4: Dashboard --
echo.
echo [4/4] Starting dashboard ...
start "Dashboard" cmd /k "cd /d "%PROJECT_ROOT%" && set OUTPUT_BASE=%OUTPUT_BASE% && python dashboard.py"

echo.
echo  ================================================
echo   Kafka, producers and dashboard are running.
echo.
echo   NOW RUN:  run_spark.bat  (in a separate window)
echo             ^-- this writes the Parquet the dashboard reads
echo.
echo   Dashboard : http://localhost:8050
echo.
echo   Watch a topic stream live:
echo     docker exec sleep-kafka kafka-console-consumer
echo       --bootstrap-server localhost:9092 --topic sleep-lifestyle
echo.
echo   To stop: run stop.bat
echo  ================================================
echo.
pause