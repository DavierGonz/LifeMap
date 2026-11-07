"""Ejecuta un solo job E1 en Spark y mide RAM/CPU sin umbral de 4 GiB."""
import json
from pathlib import Path
import subprocess
import sys
import time
from yelp import resources,ROOT

def main():
    if sys.version_info[:2]!=(3,10) or Path(sys.prefix).resolve()!=(ROOT/'.venv').resolve(): raise SystemExit('Use LifeMap .venv Python 3.10')
    job=sys.argv[1]
    if job not in {'train','analytics'}: raise SystemExit('Expected train or analytics')
    module='ml/train.py' if job=='train' else 'analytics/job.py'
    command=['docker','--context','desktop-linux','exec','lifemap-spark-master-1','/opt/spark/bin/spark-submit','--conf','spark.task.maxFailures=1','--conf','spark.sql.files.maxPartitionBytes=16777216','/opt/lifemap/'+module,*sys.argv[2:]]
    samples=[resources()];start=time.monotonic();last=start;critical=0;reason=None;stats=[]
    log=ROOT/'work'/f'spark-{job}.log'
    with log.open('w',encoding='utf-8') as output:
        proc=subprocess.Popen(command,stdout=output,stderr=subprocess.STDOUT,cwd=ROOT)
        while proc.poll() is None:
            item=resources();samples.append(item);critical=critical+1 if item['available_bytes']<512*1024**2 else 0
            if critical>=3: reason='critical_host_memory'
            if 'OutOfMemoryError' in log.read_text(encoding='utf-8',errors='replace')[-8000:]: reason='spark_oom'
            if time.monotonic()-last>15:
                result=subprocess.run(['docker','--context','desktop-linux','stats','--no-stream','--format','{{json .}}','lifemap-spark-master-1','lifemap-spark-worker-1','lifemap-mongodb-1'],capture_output=True,text=True,timeout=10)
                if result.returncode==0: stats.extend(json.loads(line) for line in result.stdout.splitlines())
                print(json.dumps({'job':job,'elapsed_s':round(time.monotonic()-start),'available_gib':item['available_gib'],'cpu_percent':item.get('windows_cpu_percent')}),flush=True)
                print(log.read_text(encoding='utf-8',errors='replace')[-350:],flush=True);last=time.monotonic()
            if reason:
                subprocess.run(['docker','--context','desktop-linux','exec','lifemap-spark-master-1','python3','-c',"import os,signal,pathlib; os.kill(int(pathlib.Path('/scratch/lifemap-e1.pid').read_text()),signal.SIGTERM)"],capture_output=True,timeout=15)
                break
            time.sleep(1)
        proc.wait(timeout=20)
    samples.append(resources())
    record={'job':job,'args':sys.argv[2:],'returncode':proc.returncode,'abort_reason':reason,'seconds':round(time.monotonic()-start,2),'minimum_available_gib':min(s['available_gib'] for s in samples),'peak_windows_cpu_percent':max(s.get('windows_cpu_percent',0) for s in samples),'before':samples[0],'after':samples[-1],'container_samples':stats}
    report=ROOT/'docs/reports/e1-execution.json';data=json.loads(report.read_text()) if report.exists() else {'jobs':[]}
    data['jobs'].append(record);report.write_text(json.dumps(data,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in record.items() if k!='container_samples'}),flush=True)
    print(log.read_text(encoding='utf-8',errors='replace')[-5000:],flush=True)
    return proc.returncode or int(bool(reason))
if __name__=='__main__': raise SystemExit(main())
