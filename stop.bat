@echo off
title Sleep Pipeline — Stop

echo.
echo  Stopping Kafka ...
docker compose down

echo.
echo  All services stopped.
echo  (Close any remaining Python terminal windows manually.)
echo.
pause
