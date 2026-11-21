"""Prueba local del modelo guardado y de alertas foreachBatch con Yelp real."""
from pathlib import Path
import json
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    scope='test_risk_'+uuid.uuid4().hex[:8];counter=0
    output=ROOT/'work'/scope;output.mkdir()
    mounts=['--mount','type=volume,src=lifemap_model_store,dst=/models,readonly',
            '--mount','type=volume,src=lifemap_dataset_cache,dst=/data',
            '--mount','type=volume,src=lifemap_stream_checkpoints,dst=/checkpoints,readonly',
            '--mount',f'type=bind,src={ROOT.as_posix()}/.secrets/mongo_writer_password.txt,dst=/run/secrets/mongo_writer_password,readonly']
    for src,target in [('ml','ml'),('streaming','streaming'),('analytics','analytics'),('config','config'),('tests/integration','integration')]:
        mounts+=['--mount',f'type=bind,src={ROOT.as_posix()}/{src},dst=/app/{target},readonly']
    def helper(action,test_scope='production'):
        cmd=['docker','run','--rm','--user','185:0','--network','lifemap_backend','--memory','192m','--cpus','0.5',*mounts,
             '--entrypoint','python','lifemap-producer:e2-v1','-m','integration.risk_phase4',action,'--scope',test_scope]
        r=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=120)
        if r.returncode:print(r.stderr[-3000:]);raise RuntimeError('Risk integration helper failed')
        return json.loads(r.stdout)
    selected=helper('prepare');print(json.dumps(selected),flush=True)
    published=helper('publish')
    def caught_up(data,target):return all(data['end_offsets'].get(p,-1)>=o for p,o in target['end_offsets'].items())
    for _ in range(60):
        real=helper('verify')
        print(json.dumps({k:real[k] for k in ['scope','risk_processed','alerts']}),flush=True)
        if real['risk_processed']==100 and caught_up(real,published):break
        time.sleep(5)
    assert real['risk_processed']==100 and real['alerts']>0 and caught_up(real,published)
    # Actual process restart, existing checkpoints; then duplicate Kafka deliveries.
    subprocess.run(['docker','compose','--profile','e2','--profile','streaming','restart','streaming'],cwd=ROOT,check=True)
    published=helper('publish')
    for _ in range(60):
        replay=helper('verify')
        if caught_up(replay,published):break
        time.sleep(5)
    assert caught_up(replay,published)
    assert replay['alert_fingerprint']==real['alert_fingerprint'] and replay['alerts']==real['alerts']
    helper('publish',scope)
    def consume(fail=False):
        nonlocal counter
        counter+=1
        cmd=['docker','compose','--profile','e2','--profile','streaming','run','--rm','--no-deps',
             '--name',f'lifemap-risk-retry-{counter}', 'streaming','/opt/lifemap/streaming/consumer.py','--scope',scope,'--available-now']
        if fail:cmd+=['--simulate-sink-failure']
        r=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=300)
        log=r.stdout+r.stderr;(output/f'retry-{counter}.log').write_text(log,encoding='utf-8')
        if fail:assert r.returncode and 'Intentional test sink failure' in log,log[-3000:]
        else:assert r.returncode==0,log[-3000:]
    consume(True);partial=helper('verify',scope)
    assert partial['alerts']>0
    consume();retry=helper('verify',scope)
    assert retry['risk_processed']==20
    assert all(retry['processed_at'][id]==value for id,value in partial['processed_at'].items())
    compact=lambda d:{k:v for k,v in d.items() if k not in ['alert_ids','processed_at']}
    report={'status':'passed','selected_real':selected,'real':compact(real),'replay_identical':True,
            'actual_consumer_restart':True,'partial_failure_alerts':partial['alerts'],'retry_scoped':compact(retry),
            'retry_preserved_prior_alert_timestamps':True,'training':json.loads((ROOT/'work/phase4-risk-training.json').read_text()),
            'real_demo_is_score_selected_not_population_precision_estimate':True}
    (ROOT/'docs/reports/e2-phase4.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':'passed','real_alerts':real['alerts'],'retry_alerts':retry['alerts']}),flush=True)


if __name__=='__main__':main()
