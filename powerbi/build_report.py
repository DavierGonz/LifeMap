"""Visuales PBIR nativos sobre el modelo importado y comprobado en Desktop.

Ejecutar con Desktop cerrado, usando .venv. No produce PBIX: esa conversión
se realiza y verifica en Power BI Desktop. No modifica permisos ni datos.
"""
import json
from pathlib import Path
import re
import sys

ROOT=Path(__file__).resolve().parent
SCHEMA='https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.9.0/schema.json'


def literal(value):
    if isinstance(value,bool):value=str(value).lower()
    elif isinstance(value,str):value="'"+value+"'"
    else:value=str(value)+'D'
    return {'expr':{'Literal':{'Value':value}}}


def field(table,name,aggregation=None):
    col={'Column':{'Expression':{'SourceRef':{'Entity':table}},'Property':name}}
    return col if aggregation is None else {'Aggregation':{'Expression':col,'Function':aggregation}}


def projection(table,name,aggregation=None):
    ref=table+'.'+name
    if aggregation is not None:ref=f'{["Sum","Avg","Count","Min","Max"][aggregation]}({ref})'
    return dict(field=field(table,name,aggregation),queryRef=ref,nativeQueryRef=name)


def role(*projections):return {'projections':list(projections)}


def main():
    assert sys.version_info[:2]==(3,10) and Path(sys.prefix).resolve()==(ROOT.parent/'.venv').resolve()
    pages=ROOT/'LifeMap-E2.Report/definition/pages'
    page=pages/json.loads((pages/'pages.json').read_text())['activePageName']
    definition=json.loads((page/'page.json').read_text())
    definition.update(displayName='LifeMap · señales y supervivencia',width=1280,height=900)
    (page/'page.json').write_text(json.dumps(definition,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    visuals=page/'visuals';visuals.mkdir(exist_ok=True)
    order=0
    def write(name,kind,x,y,w,h,title=None,query=None,objects=None):
        nonlocal order
        visual=dict(visualType=kind,objects=objects or {})
        if query:visual['query']=query
        if title:
            visual['visualContainerObjects']={'title':[{'properties':{'show':literal(True),'text':literal(title),'fontSize':literal(13)}}]}
        data=dict(name=name,position=dict(x=x,y=y,width=w,height=h,z=order,tabOrder=order),visual=visual)
        order+=1
        data['$schema']=SCHEMA
        out=visuals/name;out.mkdir(exist_ok=True);(out/'visual.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    def text(name,x,y,w,h,value,size=13):
        write(name,'textbox',x,y,w,h,objects={'general':[{'properties':{'paragraphs':[{'textRuns':[{'value':value,'textStyle':{'fontSize':f'{size}px','color':'#233d35','fontFamily':'Segoe UI'}}]}]}}]})
    text('lifemap_header',20,10,1240,45,'LifeMap E2 · Filadelfia y entorno · API real · Actualización manual',24)
    text('lifemap_method',20,55,1240,45,'2019–2022 · radio 50 km · cero hotspots reales hasta ahora · scores no calibrados; no son probabilidades poblacionales. Curvas retrospectivas desde la primera reseña.',13)
    write('hotspots_map','azureMap',20,110,600,300,'Hotspots geográficos (0 observados)',{'queryState':{
        'Category':role(projection('Hotspots','geohash')),
        'Y':role(projection('Hotspots','latitude',1)),'X':role(projection('Hotspots','longitude',1)),
        'Size':role(projection('Hotspots','reviews',0)),
        'Tooltips':role(projection('Hotspots','negative_percentage',4),projection('Hotspots','change_percentage_points',4))}})
    write('active_alerts','tableEx',640,110,620,300,'Alertas activas · score no calibrado',{'queryState':{
        'Values':role(*[projection('Alerts',name) for name in ['business_id','event_date','risk_score','model_id']])}},
        {'columnHeaders':[{'properties':{'autoSizeColumnWidth':literal(True),'columnAdjustment':literal('growToFit')}}],
         'total':[{'properties':{'totals':literal(False)}}]})
    write('kaplan_meier','lineChart',20,430,600,340,'Kaplan–Meier · American (New) y zonas',{'queryState':{
        'Category':role(projection('KaplanMeier','time_days')),'Series':role(projection('KaplanMeier','group')),
        'Y':role(projection('KaplanMeier','survival',4)),
        'Tooltips':role(*[projection('KaplanMeier',name,4) for name in ['at_risk','events','censored']])},
        'sortDefinition':{'sort':[{'field':field('KaplanMeier','time_days'),'direction':'Ascending'}]}},
        {'lineStyles':[{'properties':{'lineChartType':literal('step'),'interpolationStep':literal('after'),'strokeWidth':literal(2)}}],
         'categoryAxis':[{'properties':{'axisType':literal('Scalar'),'showAxisTitle':literal(True)}}],
         'valueAxis':[{'properties':{'start':literal(0),'end':literal(1),'showAxisTitle':literal(True)}}]})
    write('negative_evolution','lineChart',640,430,620,340,'Evolución del porcentaje negativo · 30 días',{'queryState':{
        'Category':role(projection('Evolution','start')),'Y':role(projection('Evolution','negative_percentage',4)),
        'Tooltips':role(projection('Evolution','reviews',0),projection('Evolution','negative',0))},
        'sortDefinition':{'sort':[{'field':field('Evolution','start'),'direction':'Ascending'}]}},
        {'categoryAxis':[{'properties':{'axisType':literal('Scalar')}}],
         'valueAxis':[{'properties':{'start':literal(0),'end':literal(100)}}]})
    write('refresh_evidence','tableEx',20,790,1240,90,'Fuente API · pulsa Inicio → Actualizar',{'queryState':{
        'Values':role(*[projection('Refresh',name) for name in ['api_generated_at','loaded_at_utc','hotspots','active_alerts','windows']])}},
        {'columnHeaders':[{'properties':{'autoSizeColumnWidth':literal(True),'columnAdjustment':literal('growToFit')}}]})
    model=ROOT/'LifeMap-E2.SemanticModel/definition'
    expressions=model/'expressions.tmdl'
    expressions.write_text(expressions.read_text(encoding='utf-8').replace('expression Consulta1 =','expression LifeMap ='),encoding='utf-8')
    meta=model/'model.tmdl';meta.write_text(meta.read_text(encoding='utf-8').replace('"Consulta1"','"LifeMap"'),encoding='utf-8')
    for table in ['Hotspots','Alerts','KaplanMeier','Evolution','Refresh']:
        path=model/'tables'/f'{table}.tmdl';data=path.read_text(encoding='utf-8')
        start=data.index('\t\tsource =');end=data.index('\n\tannotation PBI_NavigationStepName',start)
        data=data[:start]+f'\t\tsource = LifeMap[{table}]\n'+data[end:]
        for name in ['latitude','longitude']:
            pattern=r'(\tcolumn '+name+r'\n)(.*?)(?=\n\tcolumn |\n\tpartition )'
            def geo(m):
                section=m[2].replace('summarizeBy: sum','summarizeBy: none')
                if 'dataCategory:' not in section:section='\t\tdataCategory: '+name.title()+'\n'+section
                return m[1]+section
            data=re.sub(pattern,geo,data,flags=re.S)
        for name in ['risk_score','survival','negative_percentage','change_percentage_points','time_days']:
            pattern=r'(\tcolumn '+name+r'\n)(.*?)(?=\n\tcolumn |\n\tpartition )'
            def metric(m):
                section=m[2].replace('summarizeBy: sum','summarizeBy: none')
                if 'formatString:' not in section:section='\t\tformatString: '+('0.0%' if name=='survival' else '0.000' if name=='risk_score' else '0.00')+'\n'+section
                return m[1]+section
            data=re.sub(pattern,metric,data,flags=re.S)
        path.write_text(data,encoding='utf-8')
    print(json.dumps({'visuals':len(list(visuals.glob('*/visual.json'))),'tables':5,'source':'LifeMap API','pbix_requires_Desktop':True}))


if __name__=='__main__':main()
