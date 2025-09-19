from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, JSONResponse
import os

app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK"

@app.get("/health", response_class=JSONResponse)
def health():
    return {"ok": True}

@app.get("/diag", response_class=JSONResponse)
def diag():
    return {
        "APP_URL": os.getenv("APP_URL"),
        "DB_PATH": os.getenv("DB_PATH", "/tmp/data.sqlite"),
        "API_VER": "2025-01"
    }
