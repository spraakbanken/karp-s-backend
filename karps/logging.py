from datetime import datetime
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now().isoformat(),
            # TODO disambiguate queries from /search and /count
            # TODO /search makes many queries - could be nice to connect them in the log
            "q": getattr(record, "args")["q"],
            "execute_took_s": getattr(record, "args")["execute_took"],
            "fetchall_took_s": getattr(record, "args")["fetchall_took"],
            # log true/false depending on if the logging call was made when an exception has occurred
            "error": bool(record.exc_info),
        }
        warnings = getattr(record, "args")["warnings"]
        if warnings:
            payload["warnings"] = warnings
        return json.dumps(payload, ensure_ascii=False)


def setup_sql_logger(logging_dir: str):
    Path(logging_dir).mkdir(exist_ok=True)
    logger = logging.getLogger("sql")
    logger.propagate = False
    logger.setLevel("INFO")
    h = RotatingFileHandler(os.path.join(logging_dir, "sql.jsonl"), maxBytes=50000000)
    h.setFormatter(JSONFormatter())
    logger.addHandler(h)


def get_sql_logger():
    return logging.getLogger("sql")
