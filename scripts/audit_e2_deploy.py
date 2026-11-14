"""Auditoría local y GitHub de lectura; jamás consulta Azure ni usa SSH."""
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime,timezone

ROOT=Path(__file__).resolve().parents[1]


def run(*args):return subprocess.check_output(args,cwd=ROOT,text=True,encoding='utf-8').strip()


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    remote=run('git','ls-remote','--heads','origin')
    model=json.loads(run('docker','compose','--profile','e2','--profile','streaming','config','--format','json'))
    ids=run('docker','compose','ps','-q').splitlines();states=json.loads(run('docker','inspect',*ids))
    services=[]
    for name,s in model['services'].items():
        image=json.loads(run('docker','image','inspect',s['image']))[0]
        assert image['Os']=='linux' and image['Architecture']=='amd64'
        services.append(dict(name=name,image=s['image'],bytes=image['Size'],memory_mib=int(s['mem_limit'])//1024**2,cpu=float(s['cpus']),
                             ports=s.get('ports',[])))
    assert len(services)==10 and all(s['State']['Health']['Status']=='healthy' and not s['State']['OOMKilled'] for s in states)
    usage=json.loads(run('docker','system','df','-v','--format','{{json .}}'))
    volumes=[dict(name=v['Name'],size=v['Size']) for v in usage.get('Volumes',[]) if v.get('Name','').startswith('lifemap_')]
    for sha in ['b45f062','d3dcf86','39b9522','3a563d4','ef25d14']:
        subprocess.run(['git','merge-base','--is-ancestor',sha,'HEAD'],cwd=ROOT,check=True)
    report=dict(audited_utc=datetime.now(timezone.utc).isoformat(),remote_heads=remote.splitlines(),
       current_branch=run('git','branch','--show-current'),local_head=run('git','rev-parse','HEAD'),all_five_phases_ancestors=True,
       services=services,memory_limits_total_mib=sum(s['memory_mib'] for s in services),
       local_lifemap_volumes=volumes,azure=dict(contacted=False,powered_on=False,free_disk_actual='pending authorized SSH audit',
       memory_plan='16 GiB host; service caps + 256 MiB candidate; sequential builds; no simultaneous heavy batch jobs',
       disk_gate='Require at least 30 GiB available before backup/import; increase if 2x backup+assets exceed this reserve',
       persistent_resources_reused=True),fixtures_excluded_from_transfer=['__e2_test_*','__infra*','__yelp*'],
       no_push_or_merge=True)
    (ROOT/'work/e2-deploy-audit.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(services=len(services),linux_amd64=True,memory_limit_mib=report['memory_limits_total_mib'],volumes=volumes,azure_contacted=False)))


if __name__=='__main__':main()
