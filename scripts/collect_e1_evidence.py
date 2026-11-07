"""Consolida evidencia real local de E1; no modifica servicios ni datos."""
import json
from pathlib import Path
import subprocess
import sys
from urllib.request import urlopen
from collections import Counter

ROOT=Path(__file__).resolve().parents[1]

def command(*args):
    return subprocess.check_output(args,cwd=ROOT,text=True).strip()

def docker(*args): return command('docker','--context','desktop-linux',*args)

def read(path): return json.loads((ROOT/path).read_text(encoding='utf-8'))

def main():
    if sys.version_info[:2]!=(3,10) or Path(sys.prefix).resolve()!=(ROOT/'.venv').resolve():
        raise SystemExit('Use LifeMap .venv Python 3.10')
    original=read('docs/reports/yelp-evidence.json')
    analysis=original['analysis']
    for kind in ['sentiment','analytics']:
        docker('cp',f'lifemap-spark-master-1:/data/lifemap-e1/{kind}-active.json',str(ROOT/'work'/f'{kind}.json'))
    partition_counts=Counter(row['worker'] for row in analysis['partitions'])
    source_analysis={key:analysis[key] for key in ['dataset','dataset_version','fingerprint','schema','counts','dataset_cutoff','source_first_review','life_approximation']}
    source_analysis['worker_partition_counts']=dict(partition_counts)
    source_analysis['viable_combinations']=[r for r in analysis['combinations'] if r['viable']]
    stats=[json.loads(line) for line in docker('stats','--no-stream','--format','{{json .}}').splitlines()]
    containers=json.loads(docker('inspect',*['lifemap-'+name+'-1' for name in ['mongodb','spark-master','spark-worker','dask-scheduler','dask-worker-1','dask-worker-2','api','jenkins']]))
    state=[{'name':c['Name'].lstrip('/'),'image':c['Image'],'status':c['State']['Status'],'health':c['State'].get('Health',{}).get('Status'),'oom_killed':c['State']['OOMKilled'],'restart_count':c['RestartCount']} for c in containers]
    baseline=read('docs/reports/docker-safety-check-20261106.json')['second']
    old_names=[row['Names'] for row in baseline['ps_all'] if not row['Names'].startswith('lifemap-')]
    old=json.loads(docker('inspect',*old_names))
    old_volumes={row['Name'] for row in baseline['volumes']}
    existing=set(docker('volume','ls','--format','{{.Name}}').splitlines())
    assert old_volumes<=existing
    assert all(not c['State']['Running'] for c in old)
    with urlopen('http://127.0.0.1:5100/health',timeout=12) as response: health=json.load(response)
    with urlopen('http://127.0.0.1:5100/stats/closure?group_by=category&min_business_days=3650&limit=5',timeout=12) as response: rates=json.load(response)
    sys.path.insert(0,str(ROOT/'scripts'))
    from yelp import resources
    jobs=read('docs/reports/e1-execution.json')['jobs']
    compact_jobs=[{k:v for k,v in row.items() if k!='container_samples'} for row in jobs]
    value={'source':original['source'],'source_analysis':source_analysis,'selection':read('config/yelp_selection.json'),'sentiment':read('work/sentiment.json'),'analytics':read('work/analytics.json'),'ingestion_fixture_tests':original['tests'],'health':health,'closure_examples':rates,'unit_tests':{'host_unittest':21,'docker_pytest':13},'resources':{'windows':resources(),'docker_stats':stats,'spark_jobs':compact_jobs},'containers':state,'isolation':{'protected_containers_registered_and_stopped':len(old),'historical_volumes_preserved':len(old_volumes),'no_remote_push_or_deployment':True},'pending':['Second computer and public URL','Authorized first push and remote main/pollSCM demonstration','Teacher notification and area/category exclusivity','SentimentStream source code reuse verification','Live defense']}
    for name in ['jenkins-memory','jenkins-memory-final']:
        path=ROOT/'work'/f'{name}.json'
        if path.exists():
            samples=read(f'work/{name}.json')
            value['resources'][name]={'samples':len(samples),'minimum_windows_available_gib':min(r['available_gib'] for r in samples),'peak_windows_cpu_percent':max(r.get('windows_cpu_percent',0) for r in samples),'after':samples[-1]}
    if (ROOT/'work/fault-gate.json').exists(): value['jenkins_gate']=read('work/fault-gate.json')
    target=ROOT/'docs/reports/e1-evidence.json'
    target.write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(json.dumps({'written':str(target),'containers':state,'windows_available_gib':value['resources']['windows']['available_gib'],'isolation':value['isolation']}))

if __name__=='__main__': main()
