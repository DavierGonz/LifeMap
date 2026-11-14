"""Demo local acotada: reseñas nuevas reales y avance de watermark opcional."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
BASE='http://127.0.0.1:5100'
AREA='lat=39.9526&lon=-75.1652&radius_m=50000&limit=1'


def get(path):
    with urlopen(BASE+path,timeout=15) as response:return json.load(response)


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    p=argparse.ArgumentParser();p.add_argument('--limit',type=int,default=100);p.add_argument('--close-windows-after');a=p.parse_args()
    assert 1<=a.limit<=100
    before=get('/live/hotspots?'+AREA)
    def produce(after,limit):
        r=subprocess.run(['docker','exec','lifemap-api-1','python','-m','streaming.producer','--cutoff','2019-01-01T00:00:00Z',
             '--after',after,'--limit',str(limit),'--seconds-per-day','0.3333333333','--run-id','e2-local-final-demo'],cwd=ROOT,capture_output=True,text=True,check=True)
        data=json.loads(r.stdout);assert data['sent']==limit
        return data
    first=produce('latest',a.limit)
    for _ in range(30):
        health=get('/health')
        if health['services']['streaming']['consumer_lag_messages']==0:break
        time.sleep(3)
    else:raise RuntimeError('Consumer did not catch up')
    published=[first]
    if a.close_windows_after:
        from streaming.producer import instant
        assert instant(a.close_windows_after)>instant(first['last_date']),'Watermark driver must follow the actual source events'
        published.append(produce(a.close_windows_after,1))
    for _ in range(30):
        after=get('/live/hotspots?'+AREA);health=get('/health')
        caught=health['services']['streaming']['consumer_lag_messages']==0
        if caught and (not a.close_windows_after or after['total_windows']>before['total_windows']):break
        time.sleep(3)
    else:raise RuntimeError('Expected finalized windows were not observed')
    result=dict(real_yelp=True,synthetic_events=False,published=published,windows_before=before['total_windows'],windows_after=after['total_windows'],
                hotspots=after['total'],alerts=get('/alerts?'+AREA+'&active=true')['total'],risk_states=get('/risk/near?'+AREA)['total'],
                consumer_lag_messages=0,cutoff_model_unchanged='2019-01-01T00:00:00Z',
                source='Existing MongoDB Yelp; actual review timestamps; sample, not population prevalence')
    (ROOT/'work/e2-final-demo.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))


if __name__=='__main__':main()
