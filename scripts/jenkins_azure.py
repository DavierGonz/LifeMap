"""Administracion E1.3 por tunel SSH local; ningun secreto en Git o stdout."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET
import jenkins_e1 as local

ROOT=Path(__file__).resolve().parents[1]
SECRET=ROOT/'.secrets/jenkins-azure-admin.json'
local.URL='http://127.0.0.1:18083'
SSH=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-i',r'C:\Users\User\.ssh\vm-lifemap-e1_key.pem','azureuser@20.230.240.88']

def connect():
    if not SECRET.exists():
        password=subprocess.check_output(SSH+['sudo -n docker exec lifemap-jenkins-1 cat /var/jenkins_home/secrets/initialAdminPassword'],text=True).strip()
        client=local.Jenkins('admin',password)
        with client.request('me/descriptorByName/jenkins.security.ApiTokenProperty/generateNewToken',{'newTokenName':'lifemap-azure-e1'}) as response:
            token=json.load(response)['data']['tokenValue']
        SECRET.write_bytes((json.dumps({'username':'admin','token':token})+'\n').encode())
    client=local.Jenkins(**json.loads(SECRET.read_text()));client.get('api/json?tree=nodeName');return client

def prepare(client):
    sha=local.command('git','rev-parse','HEAD')
    bundle=ROOT/'work'/f'{sha}.bundle'
    if not bundle.exists():local.command('git','bundle','create',str(bundle),'main')
    subprocess.run(['scp',*SSH[1:-1],str(bundle),SSH[-1]+':lifemap-transfer/'+bundle.name],check=True)
    subprocess.run(SSH+['sudo -n docker exec lifemap-jenkins-1 mkdir -p /var/jenkins_home/local-sources'],check=True)
    subprocess.run(SSH+['sudo -n docker cp /home/azureuser/lifemap-transfer/'+bundle.name+' lifemap-jenkins-1:/var/jenkins_home/local-sources/'+bundle.name],check=True)
    job=ET.Element('flow-definition',{'plugin':'workflow-job'})
    ET.SubElement(job,'description').text=local.MARKER+': Azure preflight from real Git bundle; public promotion disabled'
    definition=ET.SubElement(job,'definition',{'class':'org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition'})
    ET.SubElement(definition,'script').text=(ROOT/'Jenkinsfile').read_text()
    ET.SubElement(definition,'sandbox').text='true';ET.SubElement(job,'disabled').text='false'
    properties=ET.SubElement(job,'properties');params=ET.SubElement(ET.SubElement(properties,'hudson.model.ParametersDefinitionProperty'),'parameterDefinitions')
    for name,kind,value in [('LOCAL_COMMIT','hudson.model.StringParameterDefinition',sha),('FORCE_TEST_FAILURE','hudson.model.BooleanParameterDefinition','false'),('DEPLOY_PUBLIC','hudson.model.BooleanParameterDefinition','false')]:
        item=ET.SubElement(params,kind);ET.SubElement(item,'name').text=name;ET.SubElement(item,'defaultValue').text=value
    local.put_job(client,'lifemap-azure-preflight',job)
    job=ET.Element('flow-definition',{'plugin':'workflow-job'})
    ET.SubElement(job,'description').text=local.MARKER+': GitHub main; H/2 pollSCM; enable after authorized first push'
    properties=ET.SubElement(job,'properties');triggers=ET.SubElement(ET.SubElement(properties,'org.jenkinsci.plugins.workflow.job.properties.PipelineTriggersJobProperty'),'triggers')
    ET.SubElement(ET.SubElement(triggers,'hudson.triggers.SCMTrigger'),'spec').text='H/2 * * * *'
    definition=ET.SubElement(job,'definition',{'class':'org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition'})
    scm=ET.SubElement(definition,'scm',{'class':'hudson.plugins.git.GitSCM'})
    configs=ET.SubElement(scm,'userRemoteConfigs');config=ET.SubElement(configs,'hudson.plugins.git.UserRemoteConfig')
    ET.SubElement(config,'url').text='https://github.com/DavierGonz/LifeMap.git'
    branches=ET.SubElement(scm,'branches');ET.SubElement(ET.SubElement(branches,'hudson.plugins.git.BranchSpec'),'name').text='*/main'
    ET.SubElement(definition,'scriptPath').text='Jenkinsfile';ET.SubElement(definition,'lightweight').text='false'
    ET.SubElement(job,'disabled').text='true';local.put_job(client,'lifemap-main',job)
    print(json.dumps({'preflight_commit':sha,'github_job':'lifemap-main','poll_scm':'H/2 * * * *','disabled_until_authorized_push':True}))

def status(client):
    result=client.get('job/lifemap-azure-preflight/api/json?tree=builds[number,result,building,duration,actions[parameters[name,value]],artifacts[relativePath]]')
    for b in result['builds']:
        if b['building']:continue
        try:b['junit']=client.get(f"job/lifemap-azure-preflight/{b['number']}/testReport/api/json?tree=failCount,passCount,skipCount")
        except Exception:pass
    print(json.dumps(result,indent=2))
    (ROOT/'deploy/azure/jenkins-evidence.json').write_text(json.dumps(result,indent=2),encoding='utf-8')

if __name__=='__main__':
    if sys.version_info[:2]!=(3,10) or Path(sys.prefix).resolve()!=(ROOT/'.venv').resolve():raise SystemExit('Use LifeMap .venv Python 3.10')
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['configure','prepare','run','status']);parser.add_argument('--fault',action='store_true');args=parser.parse_args();client=connect()
    if args.action=='configure':local.configure(client)
    elif args.action=='prepare':prepare(client)
    elif args.action=='status':status(client)
    else:
        with client.request('job/lifemap-azure-preflight/buildWithParameters',{'LOCAL_COMMIT':local.command('git','rev-parse','HEAD'),'DEPLOY_PUBLIC':'false','FORCE_TEST_FAILURE':str(args.fault).lower()}) as response:print('Azure preflight queued; public promotion disabled')
