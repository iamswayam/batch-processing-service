import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request

from app.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="Batch Processing Service")


@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    """Reject requests larger than 10MB to prevent memory exhaustion."""
    max_body = 10 * 1024 * 1024  # 10MB
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            body_size = int(content_length)
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"detail": "Invalid Content-Length header"},
            )
        if body_size < 0:
            return JSONResponse(
                status_code=400,
                content={"detail": "Invalid Content-Length header"},
            )
        if body_size > max_body:
            return JSONResponse(
                status_code=413,
                content={"detail": "Request body too large"},
            )
    return await call_next(request)


app.include_router(router)


@app.get("/healthz")
async def health() -> dict:
    return {"status": "ok"}
