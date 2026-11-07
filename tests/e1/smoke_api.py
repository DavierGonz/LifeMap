"""Seis endpoints con datos/modelo reales; fallo implica salida no cero."""
import argparse
import json
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError

def main(base='http://127.0.0.1:5000',expected_commit=None):
    result={}
    def call(path,payload=None,expected=200):
        request=Request(base+path,data=json.dumps(payload).encode() if payload is not None else None,headers={'Content-Type':'application/json'})
        try:
            with urlopen(request,timeout=12) as response: status=response.status;data=json.load(response)
        except HTTPError as exc: status=exc.code;data=json.load(exc)
        assert status==expected,(path,status)
        key=path if expected==200 else path+' [invalid input]'
        result[key]={'status':status}
        return data
    health=call('/health');assert health['status']=='ok' and health['commit_configured']
    if expected_commit: assert health['commit']==expected_commit
    with urlopen(base+'/',timeout=12) as response:
        html=response.read()
        assert response.status==200 and 'text/html' in response.headers['Content-Type']
        assert b'id="map"' in html and b'id="predict-form"' in html
    result['/']={'status':200,'interface':True}
    for asset in ['/static/app.js','/static/app.css','/static/vendor/leaflet/leaflet.js']:
        with urlopen(base+asset,timeout=12) as response:assert response.status==200
    near=call('/businesses/near?lat=39.9526&lon=-75.1652&radius_m=5000&category=Restaurants&limit=5')
    assert 0<len(near['businesses'])<=5
    polygon={'type':'Polygon','coordinates':[[[-75.3,39.85],[-75.0,39.85],[-75.0,40.1],[-75.3,40.1],[-75.3,39.85]]]}
    within=call('/businesses/within',{'geometry':polygon,'category':'Restaurants','limit':5});assert within['count']>0
    zone=call('/zones/summary?lat=39.9526&lon=-75.1652&radius_m=5000');assert zone['businesses']>0 and zone['reviews']>0
    rates=call('/stats/closure?group_by=geohash&min_business_days=3650&limit=5');assert rates['count']>0
    positive=call('/predict',{'text':'Excellent food and wonderful friendly service. I loved this restaurant.'})
    negative=call('/predict',{'text':'Terrible disgusting food and rude slow service. Never coming back.'})
    assert positive['model_id']==negative['model_id']
    assert positive['probabilities']!=negative['probabilities']
    assert positive['probabilities']['positive']>negative['probabilities']['positive']
    call('/businesses/near?lat=nan&lon=0&radius_m=10',expected=400)
    call('/predict',{'text':''},expected=400)
    result['commit']=health['commit'];result['model_id']=positive['model_id'];result['passed']=True
    print(json.dumps(result,indent=2))
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('base',nargs='?',default='http://127.0.0.1:5000');parser.add_argument('--expected-commit');args=parser.parse_args();main(args.base,args.expected_commit)
