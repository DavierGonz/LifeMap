"""Preparacion Dask y analisis de alcance; la carga requiere seleccion aprobada."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3

from ingestion.download import DATASET, VERSION, FILES, atomic_json, cache_lock, inspect, sha256
from ingestion.etl import ETL_VERSION, area_for, families_for

def connect_stage(path):
    db=sqlite3.connect(path)
    db.execute('PRAGMA cache_size=-16384')
    db.execute('PRAGMA temp_store=FILE')
    return db

def initialize_stage(db):
    # Solo tablas derivadas de esta preparacion local. Nunca elimina MongoDB.
    db.executescript('''
    DROP TABLE IF EXISTS businesses; DROP TABLE IF EXISTS source_business_ids;
    DROP TABLE IF EXISTS review_ids; DROP TABLE IF EXISTS life;
    CREATE TABLE businesses (id TEXT PRIMARY KEY, doc TEXT, area TEXT, families TEXT) WITHOUT ROWID;
    CREATE TABLE source_business_ids (id TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE review_ids (id TEXT PRIMARY KEY) WITHOUT ROWID;
    CREATE TABLE life (id TEXT PRIMARY KEY, n INTEGER, first TEXT, last TEXT) WITHOUT ROWID;
    ''')

def add_businesses(db,result,config):
    duplicate=0
    db.executemany('INSERT OR IGNORE INTO source_business_ids VALUES (?)',((x,) for x in result['raw_ids']))
    with Path(result['path']).open(encoding='utf-8') as stream:
        for line in stream:
            row=json.loads(line)
            cursor=db.execute('INSERT OR IGNORE INTO businesses VALUES (?,?,?,?)',(row['business_id'],json.dumps(row,ensure_ascii=False),area_for(row,config),json.dumps(families_for(row,config))))
            duplicate+=int(cursor.rowcount==0)
    db.commit()
    return duplicate

def deduplicate_reviews(db,source,target):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from ingestion.etl import review_schema
    counts=Counter();life={};temporary=Path(target).with_suffix('.tmp')
    with pq.ParquetWriter(temporary,review_schema(),compression='snappy') as writer:
        for batch in pq.ParquetFile(source).iter_batches(batch_size=2000):
            unique=[]
            for row in batch.to_pylist():
                if not db.execute('INSERT OR IGNORE INTO review_ids VALUES (?)',(row['review_id'],)).rowcount:
                    counts['duplicates']+=1;continue
                unique.append(row);counts['unique']+=1
                key=row['business_id'];date=row['date']
                if key not in life: life[key]=[0,date,date]
                item=life[key];item[0]+=1;item[1]=min(item[1],date);item[2]=max(item[2],date)
            writer.write_table(pa.Table.from_pylist(unique,schema=review_schema()))
    db.executemany('''INSERT INTO life VALUES (?,?,?,?) ON CONFLICT(id) DO UPDATE SET
        n=n+excluded.n, first=MIN(first,excluded.first), last=MAX(last,excluded.last)''',((key,*value) for key,value in life.items()))
    db.commit();temporary.replace(target)
    return dict(counts)

def ledger(counts,rules):
    remaining=counts['read'];steps=[]
    for rule in rules:
        removed=counts.get(rule,0);steps.append({'rule':rule,'before':remaining,'removed':removed,'after':remaining-removed});remaining-=removed
    return steps

BUSINESS_RULES=['invalid_json','missing_coordinates','invalid_coordinates','out_of_range_coordinates','missing_business_id','invalid_is_open','invalid_categories_type','duplicates']
REVIEW_RULES=['invalid_json','missing_review_id','missing_business_id','empty_text','invalid_date','invalid_stars','orphan_business_id','business_removed_by_cleaning','duplicates']

def combinations(db,config):
    business_counts=Counter();reviews=Counter();cities={};category_counts=Counter();area_totals=Counter()
    for document,area,family_json,n in db.execute('SELECT b.doc,b.area,b.families,COALESCE(l.n,0) FROM businesses b LEFT JOIN life l ON b.id=l.id'):
        row=json.loads(document);families=json.loads(family_json)
        area_totals[area]+=n
        cities.setdefault(area,Counter())[f"{row.get('city')}, {row.get('state')}"]+=n
        for category in row['categories']: category_counts[(area,category)]+=n
        for family in families:
            business_counts[(area,family)]+=1;reviews[(area,family)]+=n
    rows=[]
    for area in config['areas']:
        for family in config['families']:
            key=(area['id'],family);n=reviews[key]
            rows.append({'area_id':area['id'],'area_label':area['label'],'bbox':area['bbox'],'family':family,'businesses':business_counts[key],'unique_clean_reviews':n,'viable':n>=1_000_000})
    return sorted(rows,key=lambda r:r['unique_clean_reviews'],reverse=True),dict(area_totals),{key:dict(value.most_common(20)) for key,value in cities.items()},[{'area_id':a,'category':c,'reviews':n} for (a,c),n in category_counts.most_common(100)]

def prepare(root='/data'):
    import dask.bag as bag
    from dask import delayed
    from distributed import Client
    from ingestion.etl import process_partition
    source=Path(root)/f'yelp-v{VERSION}'
    manifest=json.loads((source/'manifest.json').read_text())
    config=json.loads(Path('/app/config/yelp_scope_candidates.json').read_text())
    schemas=inspect(root)
    required={'business':{'business_id','latitude','longitude','is_open','categories'},'review':{'review_id','business_id','text','date','stars'}}
    for kind,fields in required.items():
        if not fields.issubset(schemas[kind]['fields']): raise ValueError('Source schema differs from assignment')
    fingerprint=hashlib.sha256(json.dumps({'dataset':DATASET,'version':VERSION,'source':manifest['artifacts'],'etl':ETL_VERSION,'scope':config},sort_keys=True).encode()).hexdigest()
    directory=source/f'prepared-{fingerprint[:16]}';directory.mkdir(exist_ok=True)
    report_path=directory/'analysis.json'
    if report_path.exists():
        report=json.loads(report_path.read_text())
        for item in report['review_parts']:
            if sha256(item['path'])!=item['sha256']: raise ValueError('Prepared cache integrity failure')
        if sha256(directory/'stage.sqlite')!=report['stage_sha256']: raise ValueError('Prepared stage integrity failure')
        atomic_json(source/'analysis.json',report)
        print(json.dumps({'preparation_cache_hit':True,'counts':report['counts']}),flush=True)
        return report
    db=connect_stage(directory/'stage.sqlite');initialize_stage(db)
    report={'dataset':DATASET,'dataset_version':VERSION,'fingerprint':fingerprint,'etl_version':ETL_VERSION,'schema':schemas,'scope_definition':config,'directory':str(directory),'counts':{},'partitions':[],'review_parts':[]}
    work=directory/'partitions';canonical=directory/'reviews';canonical.mkdir(exist_ok=True)
    with Client('tcp://dask-scheduler:8786') as client:
        client.wait_for_workers(2,timeout=60)
        workers=sorted(client.scheduler_info()['workers'])
        if len(workers)!=2: raise RuntimeError('Expected exactly two LifeMap Dask workers')
        for kind in ['business','review']:
            partitions=bag.read_text(str(source/FILES[kind]),blocksize=8*1024**2).to_delayed()
            total=Counter();cutoff=None;first_date=None
            lookup=str(directory/'business_lookup.json') if kind=='review' else None
            for offset in range(0,len(partitions),2):
                futures=[]
                for i,part in enumerate(partitions[offset:offset+2]):
                    task=delayed(process_partition)(part,kind,offset+i,str(work),lookup)
                    futures.append(client.compute(task,workers=[workers[i]],allow_other_workers=False,retries=0))
                results=client.gather(futures)
                for future in futures: future.release()
                for result in results:
                    total.update(result['counts'])
                    if kind=='business': total['duplicates']+=add_businesses(db,result,config)
                    else:
                        target=canonical/f"review-{result['index']:05d}.parquet"
                        unique=deduplicate_reviews(db,result['path'],target)
                        total['duplicates']+=unique.get('duplicates',0)
                        report['review_parts'].append({'path':str(target),'rows':unique.get('unique',0),'sha256':sha256(target)})
                        if result['source_last_date']: cutoff=max(cutoff,result['source_last_date']) if cutoff else result['source_last_date']
                        if result['source_first_date']: first_date=min(first_date,result['source_first_date']) if first_date else result['source_first_date']
                    result.pop('raw_ids',None);report['partitions'].append(result)
                print(json.dumps({'phase':kind,'completed_partitions':min(offset+2,len(partitions)),'total_partitions':len(partitions),'read':total['read']}),flush=True)
            rules=BUSINESS_RULES if kind=='business' else REVIEW_RULES
            steps=ledger(total,rules)
            unique=steps[-1]['after']
            table='businesses' if kind=='business' else 'review_ids'
            assert db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]==unique
            report['counts'][kind]={'read':total['read'],'unique_clean':unique,'steps':steps}
            if kind=='business':
                atomic_json(directory/'business_lookup.json',{'raw_ids':[r[0] for r in db.execute('SELECT id FROM source_business_ids')],'clean_ids':[r[0] for r in db.execute('SELECT id FROM businesses')]})
            else: report.update(dataset_cutoff=cutoff,source_first_review=first_date)
    rows,areas,cities,categories=combinations(db,config)
    report.update(combinations=rows,area_review_totals=areas,top_cities_by_area=cities,top_category_counts=categories)
    report['businesses_with_reviews']=db.execute('SELECT COUNT(*) FROM life').fetchone()[0]
    report['businesses_without_reviews']=report['counts']['business']['unique_clean']-report['businesses_with_reviews']
    report['life_approximation']={'duration':'(last clean matched review - first clean matched review).total_seconds()/86400; zero for one timestamp, null if no reviews','event':'is_open == 0','censored':'is_open == 1','cutoff':'max parseable civil date in full source review file','limitations':'Review dates approximate opening/closing. Open businesses remain censored. True opening, closing and age at delayed entry are unknown; no lifetime before first review is invented.'}
    db.close();report['stage_sha256']=sha256(directory/'stage.sqlite')
    report['status']='prepared_pending_scope_approval'
    atomic_json(report_path,report);atomic_json(source/'analysis.json',report)
    print(json.dumps({'status':report['status'],'counts':report['counts'],'viable':[r for r in rows if r['viable']],'dataset_cutoff':report['dataset_cutoff']},indent=2),flush=True)
    return report

def load(root,selection_path):
    from ingestion.load import load_selected
    return load_selected(root,selection_path)

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',default='/data')
    parser.add_argument('--approved-scope')
    args=parser.parse_args()
    with cache_lock(args.root):
        if args.approved_scope: load(args.root,args.approved_scope)
        else: prepare(args.root)
