"""Contratos E2 contra Mongo real, sin alterar datos ni checkpoints."""
import json
from pathlib import Path
import sys
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode, quote

ROOT=Path(__file__).resolve().parents[1]
BASE='http://127.0.0.1:5100'
AREA=dict(lat=39.9526,lon=-75.1652,radius_m=50000)


def get(path,params=None,status=200):
    url=BASE+path+('?' + urlencode(params) if params else '')
    try:
        with urlopen(url,timeout=20) as r:code,data=r.status,json.load(r)
    except HTTPError as e:code,data=e.code,json.load(e)
    assert code==status,(path,code,data)
    return data


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    checks=[]
    h=get('/health');assert h['services']['kafka']['partitions']==3
    assert h['services']['streaming']['consumer_lag_messages']==0;checks.append('Kafka protocol/end offsets + checkpoint lag')
    a=get('/alerts',dict(AREA,active='true',limit=500));assert a['total']>0
    for r in a['results']:
        assert r['active'] and r['calibrated'] is False and 0<=r['risk_score']<=1 and 'probability' not in r
        assert r['geometry']['type']=='Point'
    first=get('/alerts',dict(AREA,limit=2));second=get('/alerts',dict(AREA,limit=2,offset=2))
    assert len({r['review_id'] for r in first['results']+second['results']})==4
    high=get('/alerts',dict(AREA,min_score=.9,limit=500));assert high['total']<=a['total']
    inactive=get('/alerts',dict(AREA,active='false'));assert not any(r['active'] for r in inactive['results'])
    narrow=get('/alerts',dict(AREA,radius_m=1000));assert narrow['total']<a['total']
    polygon={'type':'Polygon','coordinates':[[[-75.24,39.90],[-75.09,39.90],[-75.09,40.02],[-75.24,40.02],[-75.24,39.90]]]}
    poly=get('/alerts',dict(polygon=json.dumps(polygon),limit=500));assert 0<poly['total']<=a['total']
    empty=get('/alerts',dict(AREA,**{'from':'2025-01-01','to':'2026-01-01'}));assert empty['count']==0
    checks.append('alerts: radius, polygon, date, state, score, pagination')
    risk=get('/risk/near',dict(AREA,limit=2));assert risk['count']==2
    r=risk['results'][0];assert len(r['features'])==6 and not r['calibrated']
    selected=get('/risk/near',dict(AREA,business_id=r['business_id']));assert selected['count']==1
    absent=get('/risk/near',dict(AREA,business_id='not-a-real-business'));assert absent['count']==0
    checks.append('risk: real named businesses, six features, uncalibrated score, selection/empty')
    hot=get('/live/hotspots',dict(AREA,limit=500));assert hot['total_windows']>0 and not hot['synthetic_data']
    assert all(w['finalized'] and w['geometry']['type']=='Polygon' and 0<=w['negative']<=w['reviews'] for w in hot['windows'])
    old=get('/live/hotspots',dict(AREA,**{'from':'1900-01-01','to':'1901-01-01'}));assert old['total_windows']==old['total']==0
    checks.append('hotspots: genuine production windows, valid geometry, temporal empty; no fixtures')
    survival=[]
    for category in ['American (New)','American (Traditional)']:
        for method in ['observed_origin','landmark_T']:
            s=get('/survival/'+quote(category),dict(method=method));c=s['primary']
            assert c['businesses']==c['events']+c['censored'] and len(s['zones'])==2
            assert all(z['category']==category and z['curve']['businesses']<=c['businesses'] for z in s['zones'])
            assert all(x['survival']>=y['survival'] for x,y in zip(c['points'],c['points'][1:]))
            survival.append(dict(category=category,method=method,businesses=c['businesses'],zones=[dict(zone=z['zone_id'],businesses=z['curve']['businesses']) for z in s['zones']]))
    s=get('/survival/'+quote('American (New)'),dict(zone='sector_este'));assert len(s['zones'])==1
    get('/survival/not-a-real-category',status=404)
    checks.append('Kaplan-Meier: two categories/two zones/both methods, risk sets, censures, zone/category intersection')
    invalid=[('/alerts',dict(AREA,min_score='nan')),('/alerts',dict(AREA,min_score=.8,max_score=.2)),
             ('/alerts',dict(AREA,active='yes')),('/alerts',dict(AREA,polygon=json.dumps(polygon))),
             ('/alerts',dict(polygon='{}')),('/risk/near',dict(AREA,radius_m=50001)),
             ('/risk/near',dict(AREA,offset=-1)),('/live/hotspots',dict(AREA,**{'from':'2020-01-01','to':'2019-01-01'})),
             ('/live/hotspots',dict(AREA,**{'from':'bad-date'})),('/alerts',dict(AREA,limit=501)),
             ('/risk/near',dict(AREA,unexpected=1)),('/survival/'+quote('American (New)'),dict(method='unknown')),
             ('/survival/'+quote('American (New)'),dict(zone='unknown'))]
    for path,params in invalid:get(path,params,status=400)
    get('/risk/near?lat=1&lat=2&lon=3&radius_m=1',status=400)
    checks.append('14 invalid/unknown/repeated/range/polygon/date inputs rejected')
    report=dict(status='passed',checks=checks,alerts=a['total'],risk_states=risk['total'],hotspots=hot['total'],
                finalized_windows=hot['total_windows'],survival=survival,consumer_lag_messages=0)
    (ROOT/'work/phase5-api-tests.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report))


if __name__=='__main__':main()
