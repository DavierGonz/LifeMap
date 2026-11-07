"""Consultas acotadas de solo lectura para salud; sin datos ni secretos en salida."""
import json
import os
from pathlib import Path
from urllib.request import urlopen


def mongo_probe():
    try:
        from pymongo import MongoClient

        password = Path(os.environ["MONGO_READER_PASSWORD_FILE"]).read_text().strip()
        with MongoClient(
            os.environ.get("MONGO_HOST", "mongodb"),
            username=os.environ.get("MONGO_READER_USERNAME", "lifemap_reader"),
            password=password, authSource="lifemap", maxPoolSize=2,
            serverSelectionTimeoutMS=2000, connectTimeoutMS=2000, socketTimeoutMS=2000,
        ) as client:
            client.lifemap.command("ping")
        return {"status": "ok"}
    except Exception:
        return {"status": "unavailable"}


def spark_probe():
    try:
        with urlopen(os.environ["SPARK_STATUS_URL"], timeout=2) as response:
            state = json.load(response)
        workers = sum(worker.get("state") == "ALIVE" for worker in state.get("workers", []))
        ready = state.get("status") == "ALIVE" and workers >= 1
        return {"status": "ok" if ready else "unavailable", "workers": workers}
    except Exception:
        return {"status": "unavailable"}


def dask_probe():
    try:
        with urlopen(os.environ["DASK_STATUS_URL"], timeout=2) as response:
            state = json.load(response)
        workers = int(state["workers"])
        return {"status": "ok" if workers >= 2 else "unavailable", "workers": workers}
    except Exception:
        return {"status": "unavailable"}
