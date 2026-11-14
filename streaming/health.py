"""Sonda local sin secretos, servicios externos ni cambios de estado."""
import json
from pathlib import Path
import time

status = json.loads(Path('/checkpoints/production/status.json').read_text())
assert status['instance']==Path('/tmp/lifemap-stream-instance').read_text()
assert status['active'] and time.time()-status['updated_epoch'] < 120
