"""Diez endpoints y UI real; independiente de Jenkins y sin escrituras."""
import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from urllib.request import urlopen
from tests.e1.smoke_api import main as e1


def main(base,revision=None):
    with redirect_stdout(io.StringIO()):result=e1(base,revision)
    def get(path):
        with urlopen(base+path,timeout=15) as r:
            assert r.status==200
            return json.load(r)
    health=get('/health');assert health['services']['kafka']['partitions']==3
    assert health['services']['streaming']['status']=='ok'
    config=json.loads(Path('/app/config/streaming.json').read_text())
    assert health['services']['streaming']['sentiment_model_id']==config['model_id']
    area='lat=39.9526&lon=-75.1652&radius_m=50000&limit=2'
    hot=get('/live/hotspots?'+area);assert hot['total_windows']>0 and hot['synthetic_data'] is False
    alerts=get('/alerts?'+area+'&active=true');assert alerts['count']>0
    risk=get('/risk/near?'+area);assert risk['count']>0
    assert all(not r['calibrated'] and 'probability' not in r for r in alerts['results']+risk['results'])
    survival=get('/survival/American%20%28New%29');assert survival['primary']['businesses']>50 and len(survival['zones'])==2
    with urlopen(base+'/',timeout=15) as r:html=r.read();assert b'id="monitor"' in html and b'id="survival"' in html
    with urlopen(base+'/static/e2.js',timeout=15) as r:assert r.status==200
    result.update(e2=dict(hotspots=hot['total'],windows=hot['total_windows'],active_alerts=alerts['total'],risk_states=risk['total'],
                         survival_businesses=survival['primary']['businesses'],risk_score_calibrated=False),
                  academic_endpoints=10,consumer_lag_messages=health['services']['streaming']['consumer_lag_messages'])
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('base',nargs='?',default='http://127.0.0.1:5000');p.add_argument('--expected-commit');a=p.parse_args();main(a.base,a.expected_commit)
