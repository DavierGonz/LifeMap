"""Contratos E2 de solo lectura; geoconsultas reales y scores no calibrados."""
from datetime import datetime, timezone
import json
from pathlib import Path
from flask import Blueprint, jsonify, request
from werkzeug.exceptions import BadRequest, NotFound, ServiceUnavailable
from api import repository, validation
from analytics.survival import fetch_businesses, prepare, kaplan_meier

routes=Blueprint('e2',__name__)


def date_arg(value,name):
    if value is None:return None
    try:
        if len(value)>40:raise ValueError()
        date=datetime.fromisoformat(value.replace('Z','+00:00'))
        return date.replace(tzinfo=timezone.utc) if date.tzinfo is None else date.astimezone(timezone.utc)
    except (ValueError,TypeError):raise BadRequest(name+' must be an ISO UTC date') from None


def parameters(extra=(),polygon=False):
    allowed={'lat','lon','radius_m','limit','offset','from','to',*extra}
    if polygon:allowed.add('polygon')
    validation.arguments(request.args,allowed)
    a=request.args;limit=validation.integer(a.get('limit',100),'limit',1,500)
    offset=validation.integer(a.get('offset',0),'offset',0,999999)
    start,end=date_arg(a.get('from'),'from'),date_arg(a.get('to'),'to')
    if start and end and start>=end:raise BadRequest('from must precede to')
    geometry=None
    if 'polygon' in a:
        if any(k in a for k in ['lat','lon','radius_m']):raise BadRequest('Choose polygon or radius, not both')
        try:
            if len(a['polygon'])>8192:raise ValueError()
            shape=json.loads(a['polygon'])
        except (ValueError,TypeError):raise BadRequest('polygon must be valid GeoJSON JSON') from None
        geometry,_=validation.polygon({'geometry':shape})
        spatial={'$match':{'geometry':{'$geoWithin':{'$geometry':geometry}}}}
    else:
        lat=validation.number(a.get('lat'),'lat',-90,90);lon=validation.number(a.get('lon'),'lon',-180,180)
        radius=validation.number(a.get('radius_m'),'radius_m',1,50000)
        spatial={'$geoNear':{'near':{'type':'Point','coordinates':[lon,lat]},'key':'geometry',
                            'distanceField':'distance_m','maxDistance':radius,'spherical':True}}
    return spatial,limit,offset,start,end


def temporal(field,start,end):
    bounds={}
    if start:bounds['$gte']=start
    if end:bounds['$lt']=end
    return {field:bounds} if bounds else {}


def serialize(value):
    if isinstance(value,datetime):return (value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)).isoformat()
    if isinstance(value,dict):return {k:serialize(v) for k,v in value.items() if k!='_id'}
    if isinstance(value,list):return [serialize(v) for v in value]
    return value


def page(collection,spatial,query,limit,offset,sort):
    pipeline=[spatial]
    if query:pipeline.append({'$match':query})
    pipeline += [{'$sort':sort},{'$facet':{'rows':[{'$skip':offset},{'$limit':limit}], 'total':[{'$count':'n'}]}}]
    result=list(repository.database()[collection].aggregate(pipeline,maxTimeMS=5000,allowDiskUse=False))[0]
    return result['rows'],result['total'][0]['n'] if result['total'] else 0


def display_score(row):
    row=dict(row);row['risk_score']=row.pop('probability')
    row['score_kind']='uncalibrated';row['calibrated']=False
    row['longitude'],row['latitude']=row['geometry']['coordinates']
    return row


@routes.get('/live/hotspots')
def hotspots():
    spatial,limit,offset,start,end=parameters()
    query={'finalized':True}
    if start:query['end']={'$gt':start}
    if end:query['start']={'$lt':end}
    # Stored hotspots and windows are production collections only. Never expose test fixtures.
    window_spatial=json.loads(json.dumps(spatial))
    windows,total=page('stream_windows',window_spatial,query,limit,offset,{'start':1,'geohash':1})
    found,hot_total=page('hotspots',spatial,query,limit,offset,{'start':-1,'geohash':1})
    for row in found+windows:
        ring=row['geometry']['coordinates'][0]
        row['longitude']=(min(p[0] for p in ring)+max(p[0] for p in ring))/2
        row['latitude']=(min(p[1] for p in ring)+max(p[1] for p in ring))/2
    return jsonify(serialize(dict(hotspots=found,windows=windows,count=len(found),window_count=len(windows),
                    total=hot_total,total_windows=total,limit=limit,offset=offset,has_more=offset+limit<max(total,hot_total),
                    time_filter='windows overlapping [from,to); full 30-day windows',
                    generated_at=datetime.now(timezone.utc),synthetic_data=False)))


@routes.get('/alerts')
def alerts():
    spatial,limit,offset,start,end=parameters(('active','min_score','max_score','business_id'),polygon=True)
    query=temporal('event_date',start,end)
    a=request.args
    if 'active' in a:
        if a['active'] not in ['true','false']:raise BadRequest('active must be true or false')
        query['active']=a['active']=='true'
    low=validation.number(a.get('min_score',0),'min_score',0,1);high=validation.number(a.get('max_score',1),'max_score',0,1)
    if low>high:raise BadRequest('min_score must not exceed max_score')
    query['probability']={'$gte':low,'$lte':high}
    if 'business_id' in a:
        if not 1<=len(a['business_id'])<=128:raise BadRequest('Invalid business_id')
        query['business_id']=a['business_id']
    rows,total=page('alerts',spatial,query,limit,offset,{'event_date':-1,'business_id':1,'review_id':1})
    return jsonify(serialize(dict(results=[display_score(r) for r in rows],count=len(rows),total=total,
                    limit=limit,offset=offset,has_more=offset+limit<total,score_kind='uncalibrated',
                    generated_at=datetime.now(timezone.utc))))


@routes.get('/risk/near')
def risk_near():
    spatial,limit,offset,start,end=parameters(('business_id','min_score'))
    query=temporal('as_of',start,end)
    if 'business_id' in request.args:
        value=request.args['business_id']
        if not 1<=len(value)<=128:raise BadRequest('Invalid business_id')
        query['business_id']=value
    query['probability']={'$gte':validation.number(request.args.get('min_score',0),'min_score',0,1)}
    rows,total=page('risk_features',spatial,query,limit,offset,{'distance_m':1,'business_id':1})
    names={r['business_id']:r['name'] for r in repository.database().businesses.find(
            {'business_id':{'$in':[r['business_id'] for r in rows]}},{'_id':0,'business_id':1,'name':1})}
    results=[dict(display_score(r),name=names.get(r['business_id'],r['business_id'])) for r in rows]
    return jsonify(serialize(dict(results=results,count=len(rows),total=total,limit=limit,offset=offset,
                    has_more=offset+limit<total,score_kind='uncalibrated',generated_at=datetime.now(timezone.utc))))


@routes.get('/survival/<path:category>')
def survival(category):
    validation.arguments(request.args,('method','zone','run_id'))
    if not category.strip() or len(category)>128:raise BadRequest('Invalid category')
    method=request.args.get('method','observed_origin')
    if method not in ['observed_origin','landmark_T']:raise BadRequest('Invalid survival method')
    db=repository.database();runs=db.survival_results.distinct('run_id')
    run=request.args.get('run_id')
    if run is None:
        if len(runs)!=1:raise ServiceUnavailable('Select a run_id from the available survival analysis')
        run=runs[0]
    if run not in runs:raise BadRequest('Unknown run_id')
    primary=db.survival_results.find_one({'run_id':run,'group_type':'category','group_id':category,'method':method})
    if primary is None:raise NotFound('No survival analysis exists for this primary category')
    config=json.loads(Path('/app/config/survival.json').read_text())
    zones=config['zones'];selected=request.args.get('zone')
    if selected:
        zones=[z for z in zones if z['id']==selected]
        if not zones:raise BadRequest('Unknown study zone')
    comparisons=[]
    for zone in zones:
        rows=fetch_businesses(db,category=category,polygon=zone['geometry'])
        records,excluded=prepare(rows,config['landmark'] if method=='landmark_T' else None)
        curve=kaplan_meier(records,config['minimum_businesses'],config['minimum_at_risk_for_interpretation'])
        comparisons.append(dict(zone_id=zone['id'],label=zone['label'],geometry=zone['geometry'],category=category,
                                curve=curve,exclusions=excluded))
    return jsonify(serialize(dict(category=category,method=method,run_id=run,primary=primary,zones=comparisons,
                    minimum_businesses=config['minimum_businesses'],retrospective=True,allowed_as_risk_predictor=False,
                    origin='first review; not founding',true_birth_entry_age_identifiable=False,
                    generated_at=datetime.now(timezone.utc))))
