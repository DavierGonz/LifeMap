"""Publica una reseña existente (una sola vez) y comprueba API sin caché."""
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen
from urllib.parse import urlencode

ROOT=Path(__file__).resolve().parents[1]


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    cmd=['docker','run','--rm','--user','185:0','--network','lifemap_backend','--memory','192m','--cpus','0.5',
         '--mount','type=volume,src=lifemap_model_store,dst=/models,readonly',
         '--mount','type=volume,src=lifemap_dataset_cache,dst=/data',
         '--mount',f'type=bind,src={ROOT.as_posix()}/.secrets/mongo_writer_password.txt,dst=/run/secrets/mongo_writer_password,readonly']
    for name in ['streaming','ml','analytics','config','tests/integration']:
        cmd+=['--mount',f'type=bind,src={ROOT.as_posix()}/{name},dst=/app/{name if name!="tests/integration" else "integration"},readonly']
    cmd+=['--entrypoint','python','lifemap-producer:e2-v1','-m','integration.api_phase5_refresh']
    def helper(action):
        r=subprocess.run(cmd+[action],cwd=ROOT,capture_output=True,text=True,timeout=90)
        if r.returncode:raise RuntimeError(r.stderr[-1500:])
        return json.loads(r.stdout)
    selected=helper('prepare')
    query=urlencode(dict(lat=39.9526,lon=-75.1652,radius_m=50000,business_id=selected['business_id']))
    def read():
        with urlopen('http://127.0.0.1:5100/risk/near?'+query,timeout=20) as r:return json.load(r)
    before=read();published=helper('publish')
    for _ in range(36):
        after=read()
        if after['results'][0]['as_of']==selected['date']:break
        time.sleep(3)
    else:raise RuntimeError('Consumer did not update real risk state')
    verified=helper('verify');assert verified['processed']
    assert verified['businesses']==16944 and verified['reviews']==1320594 and verified['survival_results']==424
    report=dict(status='passed',published_messages=1,real_yelp_review=selected['review_id'],business_id=selected['business_id'],
                before_as_of=before['results'][0]['as_of'],after_as_of=after['results'][0]['as_of'],
                data_changed=before['results'][0]['as_of']!=after['results'][0]['as_of'],
                before_score=before['results'][0]['risk_score'],after_score=after['results'][0]['risk_score'],
                response_timestamp_advanced=after['generated_at']>before['generated_at'],verified=verified,
                powerbi_refresh='manual; this tests API transport; verify Desktop separately with powerbi/verify-desktop.ps1')
    (ROOT/'work/phase5-refresh.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report))


if __name__=='__main__':main()
