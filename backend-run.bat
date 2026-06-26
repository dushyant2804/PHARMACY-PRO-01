@echo off
setlocal EnableExtensions

cd /d "D:\pharmacy-app-v2\backend"
set "PHARMACYOS_MODE=LOCAL_MODE"
python -m uvicorn server:app --host 127.0.0.1 --port 8000
