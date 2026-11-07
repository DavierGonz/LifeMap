"""Modelo exigido en E1.5, sin logica predictiva."""
from datetime import datetime
import os
from pathlib import Path

def main_category(categories):
    specific=sorted(set(categories)-{'Restaurants','Food','Nightlife'})
    return specific[0] if specific else 'Restaurants'

def database():
    from pymongo import MongoClient
    client=MongoClient('mongodb',username='lifemap_writer',password=Path('/run/secrets/mongo_writer_password').read_text().strip(),authSource='lifemap',serverSelectionTimeoutMS=10000,maxPoolSize=2)
    return client.lifemap

def business_document(row,first,last,count,cutoff,fingerprint,primary_category=None):
    document={'_id':row['business_id']}
    document.update(row)
    document['location']={'type':'Point','coordinates':[float(row['longitude']),float(row['latitude'])]}
    document['first_review']=first
    document['last_review']=last
    document['review_count_observed']=int(count)
    document['business_days']=(last-first).total_seconds()/86400 if first and last else None
    document['event']=int(row['is_open']==0)
    document['censored']=bool(row['is_open']==1)
    document['dataset_cutoff']=cutoff
    document['entry_age_days']=None
    document['life_is_approximation']=True
    document['category_primary']=main_category(row.get('categories',[])) if primary_category=='Restaurants' else primary_category
    document['category_primary_rule']='alphabetically first non-family Yelp tag; no official primary category is provided'
    document['dataset_fingerprint']=fingerprint
    return document

def review_document(row,fingerprint):
    record={'_id':row['review_id']};record.update(row)
    if isinstance(record['date'],str): record['date']=datetime.fromisoformat(record['date'])
    record['dataset_fingerprint']=fingerprint
    return record

def write_batch(collection,records):
    from pymongo import ReplaceOne
    if not records: return {'upserted':0,'matched':0,'modified':0}
    result=collection.bulk_write([ReplaceOne({'_id':record['_id']},record,upsert=True) for record in records],ordered=False)
    return {'upserted':result.upserted_count,'matched':result.matched_count,'modified':result.modified_count}

def ensure_indexes(db,businesses='businesses',reviews='reviews'):
    db[businesses].create_index([('location','2dsphere')],name='location_2dsphere')
    db[reviews].create_index([('business_id',1),('date',1)],name='business_date')
