"""Transferencia conservadora de modelos/Kafka/checkpoints; no invoca Docker/SSH.

Ejecutar en helper Docker, montando /source:ro o /dest y /transfer. Kafka y
streaming deben estar detenidos por el operador antes del snapshot/restauración.
No modifica E1 ni sobrescribe un archivo existente con distinto contenido.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path,PurePosixPath
import shutil
import tarfile


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def model_paths(source):
    names={'sentiment-temporal.json','risk-active.json'}
    for pointer in list(names):
        manifest=json.loads((source/pointer).read_text())
        for key in ['predictor','spark_pipeline']:
            rel=PurePosixPath(manifest[key]);assert not rel.is_absolute() and '..' not in rel.parts
            path=source/rel
            assert path.exists()
            names.add(rel.as_posix())
    return sorted(names)


def pack(source,archive,kind):
    source=Path(source).resolve();archive=Path(archive)
    assert not archive.exists(),'Preserve existing archive; choose a new transfer directory'
    names=model_paths(source) if kind=='models' else ['production'] if kind=='checkpoints' else ['lifemap-e2/ci-smoke-100.json'] if kind=='cache' else [p.name for p in source.iterdir()]
    assert names and all((source/name).exists() for name in names)
    with tarfile.open(archive,'x:gz') as tar:
        for name in names:tar.add(source/name,arcname=name,recursive=True)
    return dict(file=archive.name,sha256=sha(archive),bytes=archive.stat().st_size,kind=kind)


def restore(destination,archive,kind,expected):
    destination=Path(destination).resolve();archive=Path(archive)
    assert sha(archive)==expected,'Transfer checksum differs'
    if kind=='kafka':assert not any(destination.iterdir()),'Kafka destination is not empty; refusing overwrite'
    if kind=='checkpoints':assert not (destination/'production').exists(),'Checkpoint already exists; preserve it'
    with tarfile.open(archive,'r:gz') as tar:
        members=tar.getmembers()
        assert sum(m.size for m in members)<32*1024**3,'Unexpected transfer size'
        for m in members:
            rel=PurePosixPath(m.name)
            assert not rel.is_absolute() and '..' not in rel.parts and (m.isfile() or m.isdir()),'Unsafe archive member'
            target=destination/m.name
            assert target.resolve().is_relative_to(destination),'Archive escapes volume'
            assert m.name!='sentiment-active.json','E1 active model is protected'
            if kind=='checkpoints':assert rel.parts[0]=='production'
            if kind=='models' and target.exists() and m.isfile():
                with tar.extractfile(m) as f:
                    h=hashlib.sha256()
                    for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
                assert sha(target)==h.hexdigest(),'Existing model differs: refusing overwrite'
        # Validation complete before the first write. Never extract symlinks/devices.
        for m in members:
            target=destination/m.name
            if m.isdir():target.mkdir(parents=True,exist_ok=True)
            elif not target.exists():
                target.parent.mkdir(parents=True,exist_ok=True)
                with tar.extractfile(m) as src,target.open('xb') as dst:shutil.copyfileobj(src,dst)
            if os.name!='nt':
                os.chown(target,m.uid,m.gid);os.chmod(target,m.mode)
    return dict(restored=kind,sha256=expected,existing_different_files_overwritten=False)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['pack','restore']);p.add_argument('kind',choices=['models','kafka','checkpoints','cache']);p.add_argument('archive');p.add_argument('--sha256');a=p.parse_args()
    if a.action=='pack':result=pack('/source',a.archive,a.kind)
    else:assert a.sha256;result=restore('/dest',a.archive,a.kind,a.sha256)
    print(json.dumps(result))
