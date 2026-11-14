"""Pruebas SOLO Jenkins local; nunca SSH, Azure, push ni credenciales nuevas."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import time
import xml.etree.ElementTree as ET
import jenkins_e1 as j

ROOT=Path(__file__).resolve().parents[1]
NAME='lifemap-local-e2'


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT/'.venv').resolve()
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','run','status','disable']);p.add_argument('--snapshot',action='store_true');p.add_argument('--fault',action='store_true');p.add_argument('--target',choices=['validate-only','local'],default='validate-only');a=p.parse_args()
    j.URL='http://127.0.0.1:8083';j.MARKER='Managed by LifeMap E2'
    secret=ROOT/'.secrets/jenkins-admin.json';assert secret.is_file(),'Existing local credential required'
    client=j.Jenkins(**json.loads(secret.read_text()))
    revision=j.command('git','rev-parse','HEAD')
    if a.action=='prepare':
        bundle=ROOT/'work'/f'e2-{revision}.bundle'
        if not bundle.exists():j.command('git','bundle','create',str(bundle),'HEAD')
        j.command('docker','exec','lifemap-jenkins-1','mkdir','-p','/var/jenkins_home/local-sources')
        j.command('docker','cp',str(bundle),'lifemap-jenkins-1:/var/jenkins_home/local-sources/'+revision+'.bundle')
        script=(ROOT/'Jenkinsfile').read_text()
        if a.snapshot:
            snapshot=ROOT/'work/e2-working-tree.tar';hashes={}
            files=j.command('git','ls-files','--cached','--others','--exclude-standard','-z').split('\0')
            with tarfile.open(snapshot,'w') as archive:
                for name in sorted(set(filter(None,files))):
                    file=ROOT/name
                    if not file.is_file():continue
                    assert file.parts[len(ROOT.parts)] not in ['.git','.secrets','.venv','models','data','work','checkpoints']
                    assert file.suffix not in ['.pem','.key','.pbix','.abf']
                    archive.add(file,arcname=name,recursive=False)
                    hashes[name]=hashlib.sha256(file.read_bytes()).hexdigest()
            digest=hashlib.sha256(snapshot.read_bytes()).hexdigest()
            target='/var/jenkins_home/local-sources/'+digest+'.tar'
            j.command('docker','cp',str(snapshot),'lifemap-jenkins-1:'+target)
            # Solo el job de preparación: checkout Git real seguido del árbol aún
            # no comprometido. Nunca se inyecta este overlay en Jenkinsfile/main.
            script=script.replace('git checkout -q --detach "$LOCAL_COMMIT"','git checkout -q --detach "$LOCAL_COMMIT"\n                          tar -xf '+target)
            (ROOT/'work/e2-source-hashes.json').write_text(json.dumps(hashes,indent=2))
        else:
            assert not j.command('git','status','--porcelain'),'Use a clean committed tree or --snapshot'
            info=client.get(f'job/{NAME}/api/json?tree=lastBuild[building]')
            assert not (info.get('lastBuild') or {}).get('building'),'Previous build is active'
            # Preserve the reviewed dirty snapshot workspace; checkout the final
            # commit in a fresh workspace without deleting anyone's files.
            workspace='/var/jenkins_home/workspace/'+NAME
            backup=workspace+'-snapshot-'+str(int(time.time()))
            j.command('docker','exec','lifemap-jenkins-1','sh','-c',f'if test -d {workspace}; then test ! -e {backup} && mv {workspace} {backup}; fi')
        job=ET.Element('flow-definition',{'plugin':'workflow-job'})
        ET.SubElement(job,'description').text=j.MARKER+': local E2 gates; no remote actions'+(' (reviewed working-tree overlay)' if a.snapshot else '')
        definition=ET.SubElement(job,'definition',{'class':'org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition'})
        ET.SubElement(definition,'script').text=script;ET.SubElement(definition,'sandbox').text='true';ET.SubElement(job,'disabled').text='false'
        props=ET.SubElement(job,'properties');params=ET.SubElement(ET.SubElement(props,'hudson.model.ParametersDefinitionProperty'),'parameterDefinitions')
        for name,kind,value in [('LOCAL_COMMIT','hudson.model.StringParameterDefinition',revision),('DEPLOY_TARGET','hudson.model.StringParameterDefinition','validate-only'),('FORCE_TEST_FAILURE','hudson.model.BooleanParameterDefinition','false')]:
            item=ET.SubElement(params,kind);ET.SubElement(item,'name').text=name;ET.SubElement(item,'defaultValue').text=value
        j.put_job(client,NAME,job)
        with client.request('pipeline-model-converter/validate',{'jenkinsfile':(ROOT/'Jenkinsfile').read_text()}) as response:
            validation=response.read().decode();assert 'successfully validated' in validation,validation[:1000]
        print(json.dumps(dict(job=NAME,prepared=True,snapshot=a.snapshot,base_commit=revision,declarative_syntax='valid')))
    elif a.action=='run':
        with client.request(f'job/{NAME}/buildWithParameters',dict(LOCAL_COMMIT=revision,DEPLOY_TARGET=a.target,FORCE_TEST_FAILURE=str(a.fault).lower())) as response:
            queue=response.headers['Location'].split('/queue/',1)[1]
        for _ in range(90):
            state=client.get('queue/'+queue+'api/json')
            if state.get('executable'):number=state['executable']['number'];break
            time.sleep(2)
        else:raise RuntimeError('Jenkins queue timeout')
        print(json.dumps(dict(queued=True,number=number,fault=a.fault,target=a.target)),flush=True)
        for _ in range(360):
            build=client.get(f'job/{NAME}/{number}/api/json?tree=number,result,building,duration,artifacts[relativePath]')
            if not build['building']:break
            time.sleep(3)
        else:raise RuntimeError('Local pipeline timeout')
        output=ROOT/'work'/f'e2-jenkins-{number}';output.mkdir(exist_ok=True)
        for artifact in build['artifacts']:
            path=artifact['relativePath']
            if path.endswith(('.json','.xml','.txt')):
                with client.request(f'job/{NAME}/{number}/artifact/'+path) as response:(output/Path(path).name).write_bytes(response.read())
        with client.request(f'job/{NAME}/{number}/consoleText') as response:(output/'console.log').write_bytes(response.read())
        try:build['junit']=client.get(f'job/{NAME}/{number}/testReport/api/json?tree=failCount,passCount,skipCount')
        except Exception:pass
        (output/'build.json').write_text(json.dumps(build,indent=2))
        print(json.dumps(build))
        expected='FAILURE' if a.fault else 'SUCCESS'
        if build['result']!=expected:print((output/'console.log').read_text(encoding='utf-8')[-3500:]);raise RuntimeError('Unexpected pipeline result')
    elif a.action=='disable':client.request(f'job/{NAME}/disable',{}).close();print('Local preparation job disabled; E1 jobs/credentials unchanged')
    else:print(json.dumps(client.get(f'job/{NAME}/api/json?tree=name,buildable,lastBuild[number,result,building]')))


if __name__=='__main__':main()
