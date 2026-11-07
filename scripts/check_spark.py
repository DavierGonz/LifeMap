"""Healthcheck de infraestructura: solo consulta HTTP, sin ejecutar jobs."""
import json
import sys
from urllib.request import urlopen


def main():
    with urlopen("http://127.0.0.1:8080/json/", timeout=5) as response:
        state = json.load(response)
    if sys.argv[1] == "master":
        if state.get("status") != "ALIVE":
            raise RuntimeError("Spark master is not ALIVE")
    elif state.get("masterurl") != "spark://spark-master-internal:7077":
        raise RuntimeError("Spark worker is not registered with the LifeMap master")


if __name__ == "__main__":
    main()
