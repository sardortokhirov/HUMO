pip install -r requirements.txt
mkdir -p sessions
uvicorn main:app --host 0.0.0.0 --port 2806