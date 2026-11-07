"""Limpieza por particion Dask; adapta el patron acotado de Geoespacial."""
from collections import Counter
from datetime import datetime
from functools import lru_cache
import json
import math
from pathlib import Path
import time

ETL_VERSION='yelp-e1-v1'
NULLS={'','nan','none','null','na','n/a'}

def identifier(value):
    return value.strip() if isinstance(value,str) else ''

def review_date(value):
    try:
        result=datetime.fromisoformat(value)
        return result if result.tzinfo is None else None
    except (TypeError,ValueError): return None

def clean_business(row):
    raw=[row.get('latitude'),row.get('longitude')]
    if any(v is None or str(v).strip().lower() in NULLS for v in raw): return None,'missing_coordinates'
    try:
        lat,lon=map(float,raw)
    except (TypeError,ValueError): return None,'invalid_coordinates'
    if any(isinstance(v,bool) for v in raw) or not all(map(math.isfinite,(lat,lon))): return None,'invalid_coordinates'
    if not -90<=lat<=90 or not -180<=lon<=180: return None,'out_of_range_coordinates'
    business_id=identifier(row.get('business_id'))
    if not business_id: return None,'missing_business_id'
    if row.get('is_open') not in (0,1) or isinstance(row.get('is_open'),bool): return None,'invalid_is_open'
    categories=row.get('categories')
    if categories is not None and not isinstance(categories,str): return None,'invalid_categories_type'
    result={key:row.get(key) for key in ['name','address','city','state','postal_code','stars','review_count']}
    result.update(business_id=business_id,latitude=lat,longitude=lon,is_open=int(row['is_open']),categories=sorted(set(c.strip() for c in (categories or '').split(',') if c.strip())))
    return result,None

def clean_review(row,raw_ids=None,clean_ids=None):
    review_id=identifier(row.get('review_id'));business_id=identifier(row.get('business_id'))
    if not review_id: return None,'missing_review_id'
    if not business_id: return None,'missing_business_id'
    text=row.get('text')
    if not isinstance(text,str) or not text.strip(): return None,'empty_text'
    date=review_date(row.get('date'))
    if date is None: return None,'invalid_date'
    stars=row.get('stars')
    if isinstance(stars,bool) or not isinstance(stars,(float,int)) or not math.isfinite(stars) or stars not in (1,2,3,4,5): return None,'invalid_stars'
    if raw_ids is not None and business_id not in raw_ids: return None,'orphan_business_id'
    if clean_ids is not None and business_id not in clean_ids: return None,'business_removed_by_cleaning'
    result={key:row.get(key) for key in ['user_id','useful','funny','cool']}
    result.update(review_id=review_id,business_id=business_id,text=text.strip(),date=date.isoformat(sep=' '),stars=float(stars))
    return result,None

def area_for(row,config):
    matches=[area['id'] for area in config['areas'] if area['bbox'][0]<=row['longitude']<=area['bbox'][2] and area['bbox'][1]<=row['latitude']<=area['bbox'][3]]
    if len(matches)>1: raise ValueError('Overlapping candidate areas')
    return matches[0] if matches else 'outside_candidates'

def families_for(row,config):
    categories=set(row['categories'])
    return [name for name,tokens in config['families'].items() if categories.intersection(tokens)]

@lru_cache(maxsize=1)
def business_lookup(path):
    data=json.loads(Path(path).read_text())
    return set(data['raw_ids']),set(data['clean_ids'])

def review_schema():
    import pyarrow as pa
    return pa.schema([(k,pa.string()) for k in ['review_id','business_id','user_id','text','date']]+[('stars',pa.float64())]+[(k,pa.int64()) for k in ['useful','funny','cool']])

def process_partition(lines,kind,index,output,lookup=None):
    from distributed import get_worker
    import psutil
    start=time.monotonic();counts=Counter();records=[];raw_ids=set();latest=None;earliest=None
    raw_business,clean_businesses=business_lookup(lookup) if lookup else (None,None)
    for line in lines:
        counts['read']+=1
        try:
            row=json.loads(line)
            if not isinstance(row,dict): raise ValueError()
        except (ValueError,TypeError): counts['invalid_json']+=1;continue
        if kind=='business':
            raw_id=identifier(row.get('business_id'))
            if raw_id: raw_ids.add(raw_id)
            clean,reason=clean_business(row)
        else:
            date=review_date(row.get('date'))
            if date is not None:
                latest=max(latest,date) if latest else date
                earliest=min(earliest,date) if earliest else date
            clean,reason=clean_review(row,raw_business,clean_businesses)
        if reason: counts[reason]+=1
        else: records.append(clean);counts['clean_before_duplicates']+=1
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    path=output/f'{kind}-{index:05d}.jsonl'
    if kind=='business':
        temporary=path.with_suffix('.tmp')
        with temporary.open('w',encoding='utf-8') as stream:
            for row in records: stream.write(json.dumps(row,ensure_ascii=False)+'\n')
        temporary.replace(path)
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq
        path=output/f'review-{index:05d}.parquet';temporary=path.with_suffix('.tmp')
        pq.write_table(pa.Table.from_pylist(records,schema=review_schema()),temporary,compression='snappy')
        temporary.replace(path)
    return {'index':index,'path':str(path),'counts':dict(counts),'raw_ids':sorted(raw_ids),'source_first_date':earliest.isoformat(sep=' ') if earliest else None,'source_last_date':latest.isoformat(sep=' ') if latest else None,'worker':get_worker().name,'seconds':round(time.monotonic()-start,3),'rss_bytes':psutil.Process().memory_info().rss}
