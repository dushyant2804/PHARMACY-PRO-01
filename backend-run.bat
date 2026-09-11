@echo off
setlocal EnableExtensions

cd /d "D:\pharmacy-app-v2\backend"

set "PHARMACYOS_MODE=LOCAL_MODE"
set "PHARMACYOS_LOCAL_DATA_DIR=D:\pharmacy-app-v2\local_data"
set "PHARMACYOS_LOCAL_DB_PATH=D:\pharmacy-app-v2\local_data\pharmacyos.sqlite3"
set "LOCAL_DATA_DIR=D:\pharmacy-app-v2\local_data"
set "LOCAL_DB_PATH=D:\pharmacy-app-v2\local_data\pharmacyos.sqlite3"

python -c "import online_store; print('### ONLINE_STORE:', online_store.__file__); print('### HAS_LINE_TOTAL_FIX:', 'line_total' in open(online_store.__file__, encoding='utf-8').read()); print('### HAS_CUSTOMER_RESPONSE:', '_customer_order_response' in open(online_store.__file__, encoding='utf-8').read())"

python -m uvicorn server:app --host 127.0.0.1 --port 8000