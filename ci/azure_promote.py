"""Helper efimero: promocion Caddy sin parar el listener HTTPS."""
import argparse
import json
import re
from pathlib import Path
from urllib.request import Request,urlopen

HOST='lifemap-e1-davier-2026.westus2.cloudapp.azure.com'

def replace_upstream(text,port):
    if port not in (5100,5101) or HOST not in text: raise ValueError('Unexpected proxy configuration')
    result,n=re.subn(r'reverse_proxy 127\.0\.0\.1:510[01]\b',f'reverse_proxy 127.0.0.1:{port}',text)
    if n!=1: raise ValueError('Expected exactly one LifeMap upstream')
    return result

def switch(port,revision):
    with urlopen(f'http://127.0.0.1:{port}/health',timeout=12) as response:data=json.load(response)
    if data['status']!='ok' or data['commit']!=revision:raise ValueError('Upstream health/commit mismatch')
    path=Path('/host-caddy/Caddyfile');previous=path.read_text();updated=replace_upstream(previous,port)
    def load(text):
        with urlopen(Request('http://127.0.0.1:2019/load',data=text.encode(),headers={'Content-Type':'text/caddyfile'}),timeout=20) as response:response.read()
    load(updated)
    try:path.write_text(updated)
    except Exception:load(previous);raise
    print(json.dumps({'caddy_upstream':port,'commit':revision,'healthy':True}))

def revision(value):
    if not re.fullmatch('[0-9a-f]{40}',value):raise ValueError('Expected Git commit')
    path=Path('/host-env');updated,n=re.subn(r'^DEPLOYED_COMMIT=.*$',f'DEPLOYED_COMMIT={value}',path.read_text(),flags=re.M)
    if n!=1:raise ValueError('Unexpected server .env')
    path.write_text(updated)
    print(json.dumps({'server_revision_updated':value}))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['switch','revision']);parser.add_argument('commit');parser.add_argument('--port',type=int,choices=[5100,5101]);args=parser.parse_args()
    if args.action=='switch':switch(args.port,args.commit)
    else:revision(args.commit)
