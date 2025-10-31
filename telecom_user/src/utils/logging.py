# src/config/logging.py
import structlog
import logging
import sys
from pythonjsonlogger import json

def configure_logging(log_level: str = "INFO", json_logs: bool = True):
    
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()

    logging.getLogger().setLevel(log_level)

    shared_processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_logs:
        processor = structlog.processors.JSONRenderer()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(json.JsonFormatter())
    else:
        processor = structlog.dev.ConsoleRenderer()
        handler = logging.StreamHandler(sys.stdout)

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(processor=processor)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)