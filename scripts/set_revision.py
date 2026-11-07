"""Fija en .env el SHA local real, sin tocar contrasenas ni hacer push."""
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
if sys.version_info[:2]!=(3,10) or Path(sys.prefix).resolve()!=(ROOT/'.venv').resolve(): raise SystemExit('Use LifeMap .venv')
revision=subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,capture_output=True,text=True,check=True).stdout.strip()
path=ROOT/'.env';lines=path.read_text(encoding='utf-8').splitlines()
lines=[line for line in lines if not line.startswith('DEPLOYED_COMMIT=')]+['DEPLOYED_COMMIT='+revision]
path.write_bytes(('\n'.join(lines)+'\n').encode('utf-8'))
print(revision)
