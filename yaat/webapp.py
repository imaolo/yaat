from yaat.scraper import TopMoverQueryDocSchema
from marshmallow_jsonschema import JSONSchema
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "client/dist")

app = FastAPI()

app.mount("/dist", StaticFiles(directory=STATIC_DIR), name="dist")

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

@app.get("/api/schema", response_class=JSONResponse)
def get_schema():
    # TODO return list
    return JSONSchema().dump(TopMoverQueryDocSchema())

@app.get("/heartbeat")
# TODO test
def GET_heartbeat():
    return {"status": "ok", "message": "Heartbeat acknowledged"}

def run():
  import uvicorn
  # TODO connect db?
  uvicorn.run(app, host='0.0.0.0', port=80)