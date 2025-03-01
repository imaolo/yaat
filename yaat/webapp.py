from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
client_dir = os.path.join(BASE_DIR, "client")

app = FastAPI()

app.mount("/client", StaticFiles(directory=client_dir), name="client")

@app.get("/", response_class=FileResponse)
def read_root():
  return FileResponse(os.path.join(client_dir, 'index.html'))

@app.get("/api/number", response_class=JSONResponse)
def get_number():
    return {"number": 42}

@app.get("/heartbeat")
def get_number():
    return {"status": "ok", "message": "Heartbeat acknowledged"}

CONFIG = uvicorn.Config(app, host='0.0.0.0', port=80, log_level="info")
WEBAPP = uvicorn.Server(CONFIG)