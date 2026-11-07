"""API Kaggle y cache versionada; adapta download.py de Geoespacial."""
import base64
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import time
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

DATASET = 'yelp-dataset/yelp-dataset'
VERSION = 4
API = 'https://www.kaggle.com/api/v1/datasets'
FILES = {'business': 'yelp_academic_dataset_business.json', 'review': 'yelp_academic_dataset_review.json'}

class SafeRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        result = super().redirect_request(req, fp, code, msg, headers, newurl)
        if result is not None:
            result.remove_header('Authorization')
        return result

def authorization(value):
    if value.lstrip().startswith('{'):
        pair = json.loads(value)
        return 'Basic ' + base64.b64encode(f"{pair['username']}:{pair['key']}".encode()).decode()
    return 'Bearer ' + value.strip()

def fetch(url, credential='', offset=0):
    headers = {'User-Agent':'LifeMap-academic-ingestion/1'}
    if credential:
        headers['Authorization'] = authorization(credential)
    if offset:
        headers['Range'] = f'bytes={offset}-'
    try:
        return build_opener(SafeRedirect()).open(Request(url,headers=headers),timeout=120)
    except HTTPError as exc:
        raise RuntimeError(f'Kaggle HTTP {exc.code}; configure a private token file if access requires authentication') from None

def atomic_json(path, value):
    path=Path(path); temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,indent=2,ensure_ascii=False),encoding='utf-8')
    temporary.replace(path)

def sha256(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):
            digest.update(chunk)
    return digest.hexdigest()

@contextmanager
def cache_lock(root):
    import fcntl
    root=Path(root); root.mkdir(parents=True,exist_ok=True)
    with (root/'.ingestion.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(lock,fcntl.LOCK_UN)

def download(root='/data',credential=''):
    directory=Path(root)/f'yelp-v{VERSION}'; directory.mkdir(parents=True,exist_ok=True)
    manifest_path=directory/'manifest.json'
    if manifest_path.exists():
        manifest=json.loads(manifest_path.read_text())
        if manifest['dataset']!=DATASET or manifest['version']!=VERSION:
            raise ValueError('Cache belongs to another source/version')
        for item in manifest['artifacts']:
            path=directory/item['name']
            if not path.is_file() or path.stat().st_size!=item['bytes'] or sha256(path)!=item['sha256']:
                raise ValueError('Cache integrity failure; preserve files and investigate')
        return manifest, {'cache_hit':True,'downloaded_bytes':0}
    with fetch(f'{API}/view/{DATASET}',credential) as response:
        metadata=json.load(response)
    if int(metadata['currentVersionNumber'])!=VERSION:
        raise ValueError('Source version changed; review the pinned version explicitly')
    archive=directory/'yelp.zip'
    transferred=0
    if not archive.exists():
        if shutil.disk_usage(directory).free < 25*1024**3:
            raise RuntimeError('Less than 25 GiB of disk headroom for source and prepared files')
        part=directory/'download.part'; offset=part.stat().st_size if part.exists() else 0
        with fetch(f'{API}/download/{DATASET}?datasetVersionNumber={VERSION}',credential,offset) as response:
            resume=offset>0 and response.status==206
            if resume and not response.headers.get('Content-Range','').startswith(f'bytes {offset}-'):
                raise ValueError('Unexpected resume range')
            if response.status==206 and not resume:
                raise ValueError('Unexpected partial response')
            expected=response.headers.get('Content-Length')
            last=time.monotonic()
            with part.open('ab' if resume else 'wb') as output:
                while True:
                    chunk=response.read(1024*1024)
                    if not chunk: break
                    output.write(chunk);transferred+=len(chunk)
                    if time.monotonic()-last>15:
                        print(json.dumps({'downloaded_this_run_mib':round(transferred/1024**2),'phase':'download'}),flush=True);last=time.monotonic()
            if expected and transferred!=int(expected):
                raise ValueError('Incomplete response; partial download retained for resume')
        if not zipfile.is_zipfile(part):
            raise ValueError('Kaggle response is not a ZIP archive')
        part.replace(archive)
    artifacts=[]
    with zipfile.ZipFile(archive) as zipped:
        for kind,name in FILES.items():
            members=[m for m in zipped.infolist() if Path(m.filename).name==name]
            if len(members)!=1: raise ValueError(f'Expected one {kind} JSONL file')
            member=members[0]
            if member.filename.startswith(('/', '\\')) or '..' in Path(member.filename).parts:
                raise ValueError('Unsafe archive member')
            path=directory/name
            if not path.exists():
                if shutil.disk_usage(directory).free<member.file_size+5*1024**3:
                    raise RuntimeError('Insufficient extraction disk headroom')
                temporary=path.with_suffix('.part')
                with zipped.open(member) as source, temporary.open('wb') as output:
                    shutil.copyfileobj(source,output,length=1024*1024)
                temporary.replace(path)
            if path.stat().st_size!=member.file_size:
                raise ValueError('Extracted file length mismatch')
            artifacts.append({'kind':kind,'name':name,'bytes':path.stat().st_size,'sha256':sha256(path)})
    artifacts.append({'kind':'archive','name':archive.name,'bytes':archive.stat().st_size,'sha256':sha256(archive)})
    manifest={'dataset':DATASET,'version':VERSION,'last_updated':metadata.get('lastUpdated'),'license':metadata.get('licenseName'),'source':'https://www.kaggle.com/datasets/'+DATASET,'downloaded_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'artifacts':artifacts}
    atomic_json(manifest_path,manifest)
    return manifest,{'cache_hit':False,'downloaded_bytes':transferred}

def inspect(root='/data'):
    directory=Path(root)/f'yelp-v{VERSION}'; schemas={}
    for kind,name in FILES.items():
        types={};count=0
        with (directory/name).open(encoding='utf-8') as stream:
            for line in stream:
                row=json.loads(line);count+=1
                for key,value in row.items():
                    types.setdefault(key,set()).add(type(value).__name__)
                if count==1000: break
        schemas[kind]={'sample_rows':count,'fields':{key:sorted(value) for key,value in types.items()}}
    atomic_json(directory/'schema.json',schemas)
    return schemas

if __name__=='__main__':
    import sys
    credential=sys.stdin.read().strip() if '--credential-raw-stdin' in sys.argv else (json.loads(sys.stdin.read()).get('credential','') if '--credential-stdin' in sys.argv else '')
    with cache_lock('/data'):
        manifest,transfer=download(credential=credential)
        print(json.dumps({'manifest':manifest,'transfer':transfer,'schema':inspect()},indent=2),flush=True)
