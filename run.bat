@echo off
REM Virtual muhitni yaratish va faollashtirish
python -m venv venv
call venv\Scripts\activate

REM requirements.txt dan kutubxonalarni o'rnatish
pip install -r requirements.txt

REM sessions papkasini yaratish
if not exist sessions mkdir sessions

REM API serverini 2806 portida ishga tushirish
uvicorn telegram_api_with_monitor:app --host 0.0.0.0 --port 2806