from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, JSONResponse

app = FastAPI(title="Render Boot Test")

@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK"

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/diag")
def diag():
    return {"service": "render-boot-test"}
