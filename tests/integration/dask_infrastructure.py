"""Una tarea sintetica por worker; sin datasets."""
import json
from distributed import Client

with Client('tcp://dask-scheduler:8786', timeout='10s') as client:
    client.wait_for_workers(2, timeout=30)
    workers = client.scheduler_info()['workers']
    assert {w['name'] for w in workers.values()} == {'lifemap-worker-1', 'lifemap-worker-2'}
    results = [client.submit(sum, [1, 2, 3], workers=[address], allow_other_workers=False, pure=False).result(timeout=20) for address in workers]
    assert results == [6, 6]
    print(json.dumps({'workers': 2, 'results': results, 'status': 'passed'}))
