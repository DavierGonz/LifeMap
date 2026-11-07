"""Consultas acotadas adaptadas de Geoespacial, solo base lifemap/lector."""
from datetime import datetime
from pathlib import Path
import os
from flask import current_app
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from werkzeug.exceptions import ServiceUnavailable,BadRequest

QUERY_MS=5000

def configure(app):
    password=Path(os.environ.get('MONGO_READER_PASSWORD_FILE','/run/secrets/mongo_reader_password')).read_text().strip()
    client=MongoClient(os.environ.get('MONGO_HOST','mongodb'),username='lifemap_reader',password=password,authSource='lifemap',maxPoolSize=5,serverSelectionTimeoutMS=2000,connectTimeoutMS=2000,socketTimeoutMS=7000)
    app.extensions['database']=client.lifemap

def database(): return current_app.extensions['database']

def serializable(value):
    if isinstance(value,datetime): return value.isoformat()
    if isinstance(value,dict): return {key:serializable(item) for key,item in value.items()}
    if isinstance(value,list): return [serializable(item) for item in value]
    return value

def near(point,radius,limit,category=None):
    query={'location':{'$near':{'$geometry':point,'$maxDistance':radius}}}
    if category: query['categories']=category
    try:
        return list(database().businesses.find(query,{'_id':0}).limit(limit).max_time_ms(QUERY_MS))
    except OperationFailure as error:
        if error.code in (2,16755): raise BadRequest('Invalid spherical geometry') from None
        raise

def within(geometry,limit,category=None):
    query={'location':{'$geoWithin':{'$geometry':geometry}}}
    if category: query['categories']=category
    try:
        return list(database().businesses.find(query,{'_id':0}).limit(limit).max_time_ms(QUERY_MS))
    except OperationFailure as error:
        if error.code in (2,16755): raise BadRequest('Invalid spherical geometry') from None
        raise

def zone(point,radius,category=None):
    geo={'near':point,'distanceField':'distance_m','key':'location','maxDistance':radius,'spherical':True}
    if category: geo['query']={'categories':category}
    pipeline=[{'$geoNear':geo},{'$group':{'_id':None,'businesses':{'$sum':1},'closures':{'$sum':'$event'},'business_days':{'$sum':'$business_days'},'reviews':{'$sum':'$sentiment_summary.reviews'},'negative_reviews':{'$sum':'$sentiment_summary.negative'},'positive_reviews':{'$sum':'$sentiment_summary.positive'}}},{'$project':{'_id':0}}]
    rows=list(database().businesses.aggregate(pipeline,maxTimeMS=QUERY_MS,allowDiskUse=False))
    return rows[0] if rows else {'businesses':0,'closures':0,'business_days':0,'reviews':0,'negative_reviews':0,'positive_reviews':0}

def closure(kind,minimum,limit):
    state=database().analytics_state.find_one({'_id':'active'},max_time_ms=QUERY_MS)
    if not state: raise ServiceUnavailable('Closure results are not available yet')
    return list(database().closure_stats.find({'run_id':state['run_id'],'kind':kind,'business_days':{'$gte':minimum}},{'_id':0}).sort([('closures',-1),('key',1)]).limit(limit).max_time_ms(QUERY_MS))
