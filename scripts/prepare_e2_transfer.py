"""Snapshot local reproducible. No detiene servicios, no conecta a Azure.

Requiere árbol Git limpio y Kafka/streaming previamente detenidos por operador.
El snapshot consistente aún NO se crea durante la preparación de Fase 6.
"""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
VOLUMES={'models':'lifemap_model_store','checkpoints':'lifemap_stream_checkpoints','kafka':'lifemap_kafka_data','cache':'lifemap_dataset_cache'}


def run(*args):return subprocess.check_output(args,cwd=ROOT,text=True).strip()


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    assert not run('git','status','--porcelain'),'Commit the reviewed preparation first'
    for name in ['kafka','streaming']:
        assert run('docker','inspect','--format','{{.State.Running}}',f'lifemap-{name}-1')=='false','Stop producer/Kafka/streaming consistently after authorization; no automatic stop'
    assert not run('docker','ps','--filter','label=lifemap.ci_run','-q'),'CI is still active'
    revision=run('git','rev-parse','HEAD')
    api=json.loads(run('docker','inspect','lifemap-api-1'))[0]
    assert 'DEPLOYED_COMMIT='+revision in api['Config']['Env'],'Active API does not match the committed transfer version'
    out=ROOT/'artifacts'/('e2-transfer-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    assert out.resolve().is_relative_to((ROOT/'artifacts').resolve())
    out.mkdir(parents=True,exist_ok=False,mode=0o700)
    subprocess.run(['git','archive','--format=tar.gz','-o',str(out/'code.tar.gz'),'HEAD'],cwd=ROOT,check=True)
    run('git','bundle','create',str(out/'source.bundle'),'HEAD')
    config=json.loads(run('docker','compose','--profile','e2','--profile','streaming','config','--format','json'))
    images=[config['services'][name]['image'] for name in ['api','streaming','kafka']]
    subprocess.run(['docker','image','save','-o',str(out/'images.tar'),*images],cwd=ROOT,check=True)
    for kind,volume in VOLUMES.items():
        cmd=['docker','run','--rm','--network','none','--user','0','--memory','128m','--cpus','0.5',
             '--mount',f'type=volume,src={volume},dst=/source,readonly',
             '--mount',f'type=bind,src={out.as_posix()},dst=/out',
             '--mount',f'type=bind,src={(ROOT/"deploy/azure/e2-volumes.py").as_posix()},dst=/tool.py,readonly',
             '--entrypoint','python',images[0],'/tool.py','pack',kind,f'/out/{kind}.tgz']
        subprocess.run(cmd,cwd=ROOT,check=True)
    target='/tmp/lifemap-e2-'+out.name
    run('docker','cp',str(ROOT/'deploy/azure/e2-mongo-transfer.sh'),'lifemap-mongodb-1:/tmp/lifemap-e2-transfer.sh')
    subprocess.run(['docker','exec','lifemap-mongodb-1','sh','/tmp/lifemap-e2-transfer.sh','dump',target],cwd=ROOT,check=True)
    run('docker','cp','lifemap-mongodb-1:'+target,str(out/'mongo-e2'))
    files={}
    for path in out.rglob('*'):
        if path.is_file():
            h=hashlib.sha256()
            with path.open('rb') as f:
                for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
            files[path.relative_to(out).as_posix()]=dict(sha256=h.hexdigest(),bytes=path.stat().st_size)
    manifest=dict(commit=revision,images=images,files=files,quiescent=['kafka','streaming'],e1_collections_transferred=False,
                  credentials_transferred=False,kaggle_archive_transferred=False)
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    files['manifest.json']=dict(sha256=hashlib.sha256((out/'manifest.json').read_bytes()).hexdigest())
    (out/'SHA256SUMS').write_text(''.join(f'{value["sha256"]}  {name}\n' for name,value in sorted(files.items())))
    print(json.dumps(dict(transfer=str(out),commit=revision,files=len(files),services_still_stopped=True)))


if __name__=='__main__':main()
