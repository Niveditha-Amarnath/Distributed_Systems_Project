@echo off
:: setup_topic.bat
:: ─────────────────────────────────────────────────────────────────────────────
:: Windows convenience script to create and verify the 'user-events' Kafka topic.
::
:: Usage:
::   setup_topic.bat [KAFKA_HOME] [BOOTSTRAP_SERVER]
::
:: Defaults:
::   KAFKA_HOME        = .\kafka_2.13-3.7.0
::   BOOTSTRAP_SERVER  = localhost:9092
:: ─────────────────────────────────────────────────────────────────────────────

setlocal enabledelayedexpansion

set KAFKA_HOME=%1
if "%KAFKA_HOME%"=="" set KAFKA_HOME=.\kafka_2.13-3.7.0

set BOOTSTRAP=%2
if "%BOOTSTRAP%"=="" set BOOTSTRAP=localhost:9092

set TOPIC=user-events
set PARTITIONS=3
set REPLICATION=1

echo ========================================================
echo   Kafka Topic Setup Script (Windows)
echo ========================================================
echo   Bootstrap server : %BOOTSTRAP%
echo   Topic            : %TOPIC%
echo   Partitions       : %PARTITIONS%
echo   Replication      : %REPLICATION%
echo ========================================================

:: Create topic
echo.
echo [*] Creating topic '%TOPIC%' ...
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" ^
    --bootstrap-server %BOOTSTRAP% ^
    --create ^
    --if-not-exists ^
    --topic %TOPIC% ^
    --partitions %PARTITIONS% ^
    --replication-factor %REPLICATION%

if %ERRORLEVEL% EQU 0 (
    echo [OK] Topic '%TOPIC%' created successfully.
) else (
    echo [WARN] Topic may already exist, or an error occurred.
)

:: Describe topic
echo.
echo [*] Topic details:
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" ^
    --bootstrap-server %BOOTSTRAP% ^
    --describe ^
    --topic %TOPIC%

:: List topics
echo.
echo [*] All topics:
call "%KAFKA_HOME%\bin\windows\kafka-topics.bat" ^
    --bootstrap-server %BOOTSTRAP% ^
    --list

echo.
echo ========================================================
echo   Setup complete!  Run these in separate terminals:
echo     streamlit run dashboard.py
echo     python consumer.py --name ConsumerA
echo     python consumer.py --name ConsumerB
echo     python consumer.py --name ConsumerC
echo     python producer.py --events 1000
echo ========================================================
pause
