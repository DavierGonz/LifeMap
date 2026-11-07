"""Administra solo Jenkins LifeMap; adapta el cliente de Geoespacial."""
import argparse
import base64
import http.cookiejar
import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[1]
SECRET=ROOT/'.secrets/jenkins-admin.json'
MARKER='Managed by LifeMap E1'
URL='http://127.0.0.1:8083'

def command(*args):
    return subprocess.run(args,cwd=ROOT,capture_output=True,text=True,check=True).stdout.strip()

class Jenkins:
    def __init__(self,username,token):
        self.auth='Basic '+base64.b64encode(f'{username}:{token}'.encode()).decode()
        self.opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()));self.crumb=None
    def request(self,path,data=None,content_type=None):
        headers={'Authorization':self.auth}
        if isinstance(data,dict): data=urllib.parse.urlencode(data).encode();content_type='application/x-www-form-urlencoded'
        if data is not None:
            if self.crumb is None: self.crumb=self.get('crumbIssuer/api/json')
            headers[self.crumb['crumbRequestField']]=self.crumb['crumb']
        if content_type: headers['Content-Type']=content_type
        return self.opener.open(urllib.request.Request(URL+'/'+path,data=data,headers=headers),timeout=45)
    def get(self,path):
        with self.request(path) as response: return json.load(response)

def connect():
    if not SECRET.exists():
        password=command('docker','--context','desktop-linux','exec','lifemap-jenkins-1','cat','/var/jenkins_home/secrets/initialAdminPassword')
        client=Jenkins('admin',password)
        with client.request('me/descriptorByName/jenkins.security.ApiTokenProperty/generateNewToken',{'newTokenName':'lifemap-local-e1'}) as response:
            token=json.load(response)['data']['tokenValue']
        SECRET.write_bytes((json.dumps({'username':'admin','token':token})+'\n').encode('ascii'))
        print('LifeMap Jenkins token saved privately; value withheld')
    value=json.loads(SECRET.read_text());client=Jenkins(**value)
    client.get('api/json?tree=nodeName');return client

def configure(client):
    path=next((ROOT/'.secrets'/name for name in ['kaggle-token.txt','kaggle.json'] if (ROOT/'.secrets'/name).is_file()),None)
    if path is None: raise RuntimeError('Kaggle credential is missing')
    encoded=base64.b64encode(json.dumps({'kaggle':path.read_text(encoding='utf-8-sig').strip()}).encode()).decode()
    script='''
import jenkins.model.Jenkins
import jenkins.install.InstallState
import com.cloudbees.plugins.credentials.*
import com.cloudbees.plugins.credentials.domains.Domain
import org.jenkinsci.plugins.plaincredentials.impl.StringCredentialsImpl
import hudson.util.Secret
def j=Jenkins.get()
def value=new groovy.json.JsonSlurper().parseText(new String('%s'.decodeBase64(),'UTF-8'))
def store=SystemCredentialsProvider.getInstance().getStore()
def existing=store.getCredentials(Domain.global()).find{ it.id=='lifemap-kaggle' }
def credential=new StringCredentialsImpl(CredentialsScope.GLOBAL,'lifemap-kaggle','Managed by LifeMap E1: Kaggle',Secret.fromString(value.kaggle))
if(existing) {
 if(!existing.description.startsWith('Managed by LifeMap')) throw new IllegalStateException('Unmanaged credential')
 store.updateCredentials(Domain.global(),existing,credential)
} else { store.addCredentials(Domain.global(),credential) }
j.setNumExecutors(1)
j.setInstallState(InstallState.INITIAL_SETUP_COMPLETED)
j.save()
println('CONFIGURED')
'''%encoded
    with client.request('scriptText',{'script':script}) as response:
        if 'CONFIGURED' not in response.read().decode(): raise RuntimeError('Jenkins setup failed; response withheld')
    print('Kaggle credential registered in LifeMap Jenkins; wizard completed, authentication retained')

def put_job(client,name,root):
    exists=False
    try:
        info=client.get('job/'+name+'/api/json?tree=description')
        if not (info.get('description') or '').startswith(MARKER): raise RuntimeError('Refusing to replace unmanaged job')
        exists=True
    except urllib.error.HTTPError as exc:
        if exc.code!=404: raise
    path='job/'+name+'/config.xml' if exists else 'createItem?name='+name
    client.request(path,ET.tostring(root),'application/xml').close()

def prepare(client):
    revision=command('git','rev-parse','HEAD')
    bundle=ROOT/'work'/f'{revision}.bundle'
    if not bundle.exists(): command('git','bundle','create',str(bundle),'main')
    command('docker','--context','desktop-linux','exec','lifemap-jenkins-1','mkdir','-p','/var/jenkins_home/local-sources')
    command('docker','--context','desktop-linux','cp',str(bundle),'lifemap-jenkins-1:/var/jenkins_home/local-sources/'+bundle.name)
    local=ET.Element('flow-definition',{'plugin':'workflow-job'})
    ET.SubElement(local,'description').text=MARKER+': real local Git checkout; no push'
    definition=ET.SubElement(local,'definition',{'class':'org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition','plugin':'workflow-cps'})
    ET.SubElement(definition,'script').text=(ROOT/'Jenkinsfile').read_text()
    ET.SubElement(definition,'sandbox').text='true';ET.SubElement(local,'disabled').text='false'
    properties=ET.SubElement(local,'properties')
    params=ET.SubElement(ET.SubElement(properties,'hudson.model.ParametersDefinitionProperty'),'parameterDefinitions')
    for name,kind,value in [('LOCAL_COMMIT','hudson.model.StringParameterDefinition',revision),('FORCE_TEST_FAILURE','hudson.model.BooleanParameterDefinition','false')]:
        item=ET.SubElement(params,kind);ET.SubElement(item,'name').text=name;ET.SubElement(item,'defaultValue').text=value
    put_job(client,'lifemap-local-e1',local)
    remote=ET.Element('flow-definition',{'plugin':'workflow-job'})
    ET.SubElement(remote,'description').text=MARKER+': GitHub main + pollSCM; enable only after authorized first push'
    properties=ET.SubElement(remote,'properties')
    triggers=ET.SubElement(ET.SubElement(properties,'org.jenkinsci.plugins.workflow.job.properties.PipelineTriggersJobProperty'),'triggers')
    ET.SubElement(ET.SubElement(triggers,'hudson.triggers.SCMTrigger'),'spec').text='H/2 * * * *'
    definition=ET.SubElement(remote,'definition',{'class':'org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition','plugin':'workflow-cps'})
    scm=ET.SubElement(definition,'scm',{'class':'hudson.plugins.git.GitSCM','plugin':'git'})
    configs=ET.SubElement(scm,'userRemoteConfigs');config=ET.SubElement(configs,'hudson.plugins.git.UserRemoteConfig')
    ET.SubElement(config,'url').text='https://github.com/DavierGonz/LifeMap.git'
    branches=ET.SubElement(scm,'branches');ET.SubElement(ET.SubElement(branches,'hudson.plugins.git.BranchSpec'),'name').text='*/main'
    ET.SubElement(definition,'scriptPath').text='Jenkinsfile';ET.SubElement(definition,'lightweight').text='false'
    ET.SubElement(remote,'disabled').text='true'
    put_job(client,'lifemap-main',remote)
    print(json.dumps({'local_commit':revision,'local_job':'lifemap-local-e1','remote_job':'lifemap-main','remote_disabled_pending_authorized_push':True}))

def run(client,fault):
    revision=command('git','rev-parse','HEAD')
    with client.request('job/lifemap-local-e1/buildWithParameters',{'LOCAL_COMMIT':revision,'FORCE_TEST_FAILURE':str(fault).lower()}) as response:
        print(json.dumps({'queued':True,'queue_path':urllib.parse.urlsplit(response.headers['Location']).path,'fault':fault}))

def status(client):
    result=client.get('job/lifemap-local-e1/api/json?tree=builds[number,result,building,url,actions[parameters[name,value]],artifacts[relativePath]]')
    (ROOT/'docs/reports/e1-jenkins.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))

if __name__=='__main__':
    if sys.version_info[:2]!=(3,10) or Path(sys.prefix).resolve()!=(ROOT/'.venv').resolve(): raise SystemExit('Use LifeMap .venv')
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['configure','prepare','run','status']);parser.add_argument('--fault',action='store_true');args=parser.parse_args()
    client=connect()
    if args.action=='configure': configure(client)
    elif args.action=='prepare': prepare(client)
    elif args.action=='run': run(client,args.fault)
    else: status(client)
