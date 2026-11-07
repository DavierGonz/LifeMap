const fs=require('node:fs'); const path=require('node:path'); const {spawn}=require('node:child_process');
const assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../..'); const base=process.argv[2]||'http://127.0.0.1:5102';
const output=path.join(root,'work/ui-browser'); fs.mkdirSync(output,{recursive:true});
const profile=fs.mkdtempSync(path.join(output,'chrome-'));
const chrome=spawn('C:/Program Files/Google/Chrome/Application/chrome.exe',['--headless=new','--disable-gpu','--no-first-run','--no-default-browser-check','--user-data-dir='+profile,'--remote-debugging-port=0','about:blank'],{stdio:['ignore','ignore','pipe'],windowsHide:true});
let diagnostic='';chrome.stderr.on('data',d=>diagnostic=(diagnostic+String(d)).slice(-2000));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let ws;let next=0;const pending=new Map();const errors=[];const responses=[];
function send(method,params={}){return new Promise((resolve,reject)=>{const id=++next; const timer=setTimeout(()=>{pending.delete(id);reject(new Error('Timeout '+method));},20000); pending.set(id,{resolve,reject,timer}); ws.send(JSON.stringify({id,method,params}));});}
async function evaluate(expression){const r=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw new Error(JSON.stringify(r.exceptionDetails));return r.result.value;}
async function wait(expression,timeout=20000){const start=Date.now();while(Date.now()-start<timeout){if(await evaluate(expression))return;await sleep(100);}throw new Error('Condition timed out: '+expression);}
async function shot(name){const r=await send('Page.getLayoutMetrics');const clip=r.cssContentSize||r.contentSize;const s=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:true,clip:{x:0,y:0,width:clip.width,height:clip.height,scale:1}});fs.writeFileSync(path.join(output,name),Buffer.from(s.data,'base64'));}
(async()=>{
 try{
  let port;for(let i=0;i<100;i++){try{port=fs.readFileSync(path.join(profile,'DevToolsActivePort'),'utf8').split('\n')[0];if(port)break;}catch{}await sleep(100);}
  assert(port,'Chrome did not start '+diagnostic);
  const tabs=await (await fetch('http://127.0.0.1:'+port+'/json/list')).json();const tab=tabs.find(t=>t.type==='page');assert(tab);
  ws=new WebSocket(tab.webSocketDebuggerUrl);await new Promise((r,j)=>{ws.onopen=r;ws.onerror=j;});
  ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.id){const p=pending.get(m.id);if(p){clearTimeout(p.timer);pending.delete(m.id);m.error?p.reject(new Error(JSON.stringify(m.error))):p.resolve(m.result);}}else if(m.method==='Runtime.exceptionThrown'){errors.push(m.params.exceptionDetails.text+' '+(m.params.exceptionDetails.exception?.description||''));}else if(m.method==='Network.responseReceived'){const r=m.params.response;if(r.url.startsWith(base))responses.push({url:r.url,status:r.status});}};
  await send('Page.enable');await send('Runtime.enable');await send('Network.enable');
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1080,deviceScaleFactor:1,mobile:false});
  await send('Page.navigate',{url:base+'/'});
  await wait("document.querySelectorAll('.business-row').length>0 && document.querySelectorAll('.chart-row').length===6");
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'Desktop horizontal overflow');
  const initial=await evaluate("({count:document.querySelectorAll('.business-row').length,name:document.querySelector('#business-detail h3').textContent,markers:document.querySelectorAll('.leaflet-overlay-pane .leaflet-interactive').length,tiles:document.querySelectorAll('.leaflet-tile-loaded').length})");
  assert(initial.count>0 && initial.markers>=initial.count,'Actual map markers absent');
  await wait("document.querySelectorAll('.leaflet-tile').length>0 && [...document.querySelectorAll('.leaflet-tile')].every(i=>i.complete && i.naturalWidth>0)");
  initial.tiles=await evaluate("document.querySelectorAll('.leaflet-tile-loaded').length");
  await shot('desktop.png');
  const second=await evaluate("document.querySelectorAll('.business-copy strong')[1].textContent");
  await evaluate("document.querySelectorAll('.business-row')[1].click()");
  assert.equal(await evaluate("document.querySelector('#business-detail h3').textContent"),second);
  const markerName=await evaluate("document.querySelector('.leaflet-overlay-pane .leaflet-interactive').getAttribute('aria-label')");
  await evaluate("document.querySelector('.leaflet-overlay-pane .leaflet-interactive').dispatchEvent(new MouseEvent('click',{bubbles:true,view:window}))");
  await wait("document.querySelector('#business-detail h3').textContent==="+JSON.stringify(markerName));
  await evaluate("document.getElementById('category').value='Pizza';document.getElementById('limit').value='20';document.getElementById('radius').value='1500';document.getElementById('search-form').requestSubmit()");
  await wait("!document.getElementById('search-button').disabled");
  const pizza=await evaluate("({count:document.querySelectorAll('.business-row').length,name:document.querySelector('#business-detail h3')?.textContent})");
  const exact=await (await fetch(base+'/businesses/near?lat=39.9526&lon=-75.1652&radius_m=1500&limit=20&category=Pizza')).json();
  assert.equal(pizza.count,exact.count);assert.equal(pizza.name,exact.businesses[0].name);
  await evaluate("document.getElementById('category').value='LifeMap_NoSuchCategory_Zero';document.getElementById('search-form').requestSubmit()");
  await wait("!document.getElementById('search-button').disabled");
  assert.equal(await evaluate("document.querySelectorAll('.business-row').length"),0);
  await evaluate("document.getElementById('category').value='';document.getElementById('location').value='39.969,-75.134';document.getElementById('location').dispatchEvent(new Event('change'))");
  await wait("!document.getElementById('search-button').disabled && document.querySelectorAll('.business-row').length>0");
  assert.equal(await evaluate("Number(document.getElementById('latitude').value)"),39.969);
  await evaluate("document.querySelector('[data-group=geohash]').click()");
  await wait("document.querySelector('.chart-label span')?.textContent.startsWith('Zona ')");
  await evaluate("document.getElementById('review-text').value='Excellent food and wonderful friendly service. I loved this restaurant.';document.getElementById('review-text').dispatchEvent(new Event('input'));document.getElementById('predict-form').requestSubmit()");
  await wait("!document.getElementById('predict-button').disabled && document.querySelector('.prediction-heading strong')");
  const positive=await evaluate("document.querySelector('.prediction-heading strong').textContent");assert.equal(positive,'Positiva');
  await evaluate("document.getElementById('review-text').value='Terrible disgusting food and rude slow service. Never coming back.';document.getElementById('predict-form').requestSubmit()");
  await wait("!document.getElementById('predict-button').disabled");
  const negative=await evaluate("document.querySelector('.prediction-heading strong').textContent");assert.equal(negative,'Negativa');
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
  await wait('innerWidth===390');await sleep(300);
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth'),true,'Mobile horizontal overflow');
  await shot('mobile.png');
  assert.equal(errors.length,0,'Browser JS errors: '+errors.join('; '));
  const result={base,initial,pizza_count:pizza.count,selection:true,marker_selection:true,filters_match_real_api:true,empty_state:true,location_change:true,closure_groups:true,positive,negative,mobile_width:390,horizontal_overflow:false,js_errors:errors,requests:responses,passed:true};
  fs.writeFileSync(path.join(output,base.startsWith('https')?'public-tests.json':'local-tests.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result));
 }catch(error){console.log('DEBUG',JSON.stringify({errors,responses,dom:await evaluate("({title:document.title,body:document.body.innerText.slice(-5000),messages:document.querySelector('#search-message')?.textContent})")}));await shot('failed.png');throw error;}finally{if(ws){try{await send('Browser.close');}catch{}ws.close();}chrome.kill();}
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
