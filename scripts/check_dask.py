"""Healthcheck RPC y registro; no envia tareas ni procesa datos."""
import sys


def main():
    from distributed import Client

    with Client("tcp://dask-scheduler:8786", timeout="5s", set_as_default=False) as client:
        workers = client.scheduler_info()["workers"]
        if len(sys.argv) > 1 and not any(
            worker["name"] == sys.argv[1] for worker in workers.values()
        ):
            raise RuntimeError("LifeMap worker is not registered")


if __name__ == "__main__":
    main()
