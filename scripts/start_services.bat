@echo off
REM Start all MDS services in separate windows (Windows)
start "MDS Ingestion" cmd /k "cd /d %~dp0.. && .venv\Scripts\python services\ingestion\main.py"
timeout /t 3 /nobreak >nul
start "MDS Stream Processor" cmd /k "cd /d %~dp0.. && .venv\Scripts\python services\stream_processor\main.py"
start "MDS Delayed Feed" cmd /k "cd /d %~dp0.. && .venv\Scripts\python services\delayed_feed\main.py"
start "MDS Reconciliation" cmd /k "cd /d %~dp0.. && .venv\Scripts\python services\reconciliation\main.py"
start "MDS AI Analytics" cmd /k "cd /d %~dp0.. && .venv\Scripts\python services\ai_analytics\anomaly_detector.py"
start "MDS API" cmd /k "cd /d %~dp0.. && .venv\Scripts\python services\api\main.py"
echo All services started. Open Grafana at http://localhost:3000 (admin / mds_admin)
