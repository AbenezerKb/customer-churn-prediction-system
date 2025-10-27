import structlog
from fastapi import FastAPI
from src.controllers.routes.call_session_controller import router as call_session_router
from src.utils.logging import configure_logging
from src.controllers.middleware.middleware import RequestIDMiddleware


configure_logging(log_level="INFO", json_logs=True)

log = structlog.get_logger()

app = FastAPI()
app.add_middleware(RequestIDMiddleware)
app.include_router(call_session_router, prefix="/api", tags=["Call Sessions"])

@app.get("/")
def root():
    log.info("root_endpoint_called", method="GET", path="/")
    return {"message": "Telecom API"}
