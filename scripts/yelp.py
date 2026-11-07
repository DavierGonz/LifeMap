"""Lanzador Windows: .venv, secretos por stdin y consumo medido, sin servicios nuevos."""
import argparse
import ctypes
from datetime import datetime,timezone
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parents[1]

class Memory(ctypes.Structure):
    _fields_=[('length',ctypes.c_ulong),('load',ctypes.c_ulong)]+[(name,ctypes.c_ulonglong) for name in ('total','available','page_total','page_available','virtual_total','virtual_available','extended')]

last_cpu=None
def resources():
    global last_cpu
    value=Memory();value.length=ctypes.sizeof(value)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(value)): raise ctypes.WinError()
    result={'utc':datetime.now(timezone.utc).isoformat(),'available_bytes':value.available,'available_gib':round(value.available/1024**3,3),'load_percent':value.load}
    times=[ctypes.c_ulonglong() for _ in range(3)]
    if ctypes.windll.kernel32.GetSystemTimes(*(ctypes.byref(x) for x in times)):
        current=[x.value for x in times]
        if last_cpu:
            idle,kernel,user=[x-y for x,y in zip(current,last_cpu)]
            result['windows_cpu_percent']=round(100*(1-idle/(kernel+user)),1) if kernel+user else 0
        last_cpu=current
    return result

def docker(*args,**kwargs):
    return subprocess.run(['docker','--context','desktop-linux',*args],capture_output=True,text=True,check=True,timeout=20,**kwargs).stdout

def main():
    if sys.version_info[:2]!=(3,10) or Path(sys.prefix).resolve()!=(ROOT/'.venv').resolve():
        raise SystemExit('Use D:\\Proyects\\LifeMap\\.venv\\Scripts\\python.exe')
    parser=argparse.ArgumentParser()
    parser.add_argument('operation',choices=['download','analyze','test','load'])
    parser.add_argument('--approved-scope',type=Path)
    args=parser.parse_args()
    if args.operation=='load' and (not args.approved_scope or not args.approved_scope.is_file()):
        parser.error('Definitive load requires an existing --approved-scope file; never choose a scope automatically')
    module={'download':'download','analyze':'run','test':'test','load':'run'}[args.operation]
    names=['lifemap-dask-scheduler-1','lifemap-dask-worker-1-1','lifemap-dask-worker-2-1']
    baseline=json.loads(docker('inspect',*names))
    if not all(c['State']['Running'] for c in baseline): raise SystemExit('Start the three LifeMap Dask services first')
    # El coordinador debe compartir caché persistente con ambos workers.
    for container in baseline:
        if not any(m['Type']=='volume' and m['Name']=='lifemap_dataset_cache' and m['Destination']=='/data' for m in container['Mounts']):
            raise SystemExit('Missing persistent shared dataset_cache mount')
    restart_counts={c['Name']:c['RestartCount'] for c in baseline}
    command=['docker','--context','desktop-linux','exec','-i',names[0],'python','-u','-c',"import os,pathlib,runpy; pathlib.Path('/scratch/lifemap-yelp.pid').write_text(str(os.getpid())); runpy.run_module('ingestion."+module+"',run_name='__main__')"]
    payload=''
    if args.operation=='download':
        credential=''
        for name in ['kaggle-token.txt','kaggle.json']:
            path=ROOT/'.secrets'/name
            if path.is_file(): credential=path.read_text(encoding='utf-8-sig').strip();break
        command.append('--credential-stdin');payload=json.dumps({'credential':credential});credential=''
    if args.operation=='load':
        selection=json.loads(args.approved_scope.read_text(encoding='utf-8-sig'))
        if selection.get('status')!='approved_by_user': raise SystemExit('Scope has not been approved')
        docker('cp',str(args.approved_scope.resolve()),names[0]+':/scratch/yelp-approved-scope.json')
        command.extend(['--approved-scope','/scratch/yelp-approved-scope.json'])
    (ROOT/'work').mkdir(exist_ok=True)
    log=ROOT/'work'/f'yelp-{module}.log';samples=[resources()];stats=[];critical=0;reason=None;start=time.monotonic();last=start
    with log.open('w',encoding='utf-8') as stream:
        process=subprocess.Popen(command,cwd=ROOT,stdin=subprocess.PIPE,stdout=stream,stderr=subprocess.STDOUT,text=True)
        process.stdin.write(payload);process.stdin.close();payload=''
        while process.poll() is None:
            sample=resources();samples.append(sample)
            critical=critical+1 if sample['available_bytes']<512*1024**2 else 0
            if critical>=3: reason='critical_host_memory'
            if time.monotonic()-last>=15:
                state=json.loads(docker('inspect',*names))
                if any(not c['State']['Running'] or c['State']['OOMKilled'] or c['RestartCount']!=restart_counts[c['Name']] for c in state): reason='container_stopped_oom_or_unexpected_restart'
                stats.extend(json.loads(line) for line in docker('stats','--no-stream','--format','{{json .}}',*names).splitlines())
                print(json.dumps({'operation':args.operation,'elapsed_s':round(time.monotonic()-start),'available_gib':sample['available_gib'],'windows_cpu_percent':sample.get('windows_cpu_percent')}),flush=True)
                print(log.read_text(encoding='utf-8',errors='replace')[-400:],flush=True);last=time.monotonic()
            if reason:
                docker('exec',names[0],'python','-c',"import os,signal,pathlib; os.kill(int(pathlib.Path('/scratch/lifemap-yelp.pid').read_text()),signal.SIGTERM)")
                break
            time.sleep(1)
        process.wait(timeout=20)
    samples.append(resources())
    record={'operation':args.operation,'returncode':process.returncode,'abort_reason':reason,'seconds':round(time.monotonic()-start,2),'before':samples[0],'after':samples[-1],'minimum_available_gib':min(s['available_gib'] for s in samples),'peak_windows_cpu_percent':max(s.get('windows_cpu_percent',0) for s in samples),'container_samples':stats}
    evidence=ROOT/'docs/reports/yelp-execution.json'
    data=json.loads(evidence.read_text()) if evidence.exists() else {'operations':[]}
    data['operations'].append(record);evidence.write_text(json.dumps(data,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in record.items() if k!='container_samples'}),flush=True)
    print(log.read_text(encoding='utf-8',errors='replace')[-5000:],flush=True)
    return process.returncode or int(bool(reason))

if __name__=='__main__': raise SystemExit(main())
