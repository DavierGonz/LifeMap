"""Carga definitiva por lotes, bloqueada hasta aprobar area y familia."""
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path

from ingestion.download import VERSION, atomic_json, sha256
from ingestion.scope import validate_selection
from storage.yelp import database, business_document, review_document, write_batch, ensure_indexes

def load_partition(path,selected_path,fingerprint,collection_name='reviews'):
    import pyarrow.parquet as pq
    from distributed import get_worker
    if collection_name not in {'reviews','__yelp_ingestion_test_reviews'}:
        raise ValueError('Only LifeMap ingestion targets are allowed')
    selected=set(json.loads(Path(selected_path).read_text()))
    db=database();counts=Counter()
    try:
        for block in pq.ParquetFile(path).iter_batches(batch_size=500):
            records=[review_document(row,fingerprint) for row in block.to_pylist() if row['business_id'] in selected]
            counts.update(write_batch(db[collection_name],records));counts['selected']+=len(records)
    finally: db.client.close()
    return {'counts':dict(counts),'worker':get_worker().name}

def load_selected(root,selection_path):
    from dask import delayed
    from distributed import Client
    from ingestion.run import connect_stage
    source=Path(root)/f'yelp-v{VERSION}'
    analysis=json.loads((source/'analysis.json').read_text())
    selection=json.loads(Path(selection_path).read_text())
    choice=validate_selection(selection,analysis)
    directory=Path(analysis['directory']);stage_path=directory/'stage.sqlite'
    if sha256(stage_path)!=analysis['stage_sha256']: raise ValueError('Stage checksum mismatch')
    for part in analysis['review_parts']:
        if sha256(part['path'])!=part['sha256']: raise ValueError('Prepared review checksum mismatch')
    run_id=hashlib.sha256(json.dumps(selection,sort_keys=True).encode()).hexdigest()
    fingerprint=analysis['fingerprint'];db=database();stage=connect_stage(stage_path)
    try:
        previous=db.ingestion_runs.find_one({'_id':run_id,'status':'complete'})
        if previous:
            proof={name:db[name].count_documents({},hint='_id_') for name in ['businesses','reviews']}
            if proof==previous['verification'] and proof=={'businesses':choice['businesses'],'reviews':choice['unique_clean_reviews']}:
                ensure_indexes(db)
                print(json.dumps({'load_cache_hit':True,'verification':proof}),flush=True)
                return previous
            raise ValueError('Completed load counts changed; investigate before overwriting enriched records')
        if db.ingestion_runs.find_one({'kind':'yelp_load','_id':{'$ne':run_id}}):
            raise ValueError('Another definitive scope exists; review explicitly before mixing scopes')
        if any(db[name].count_documents({'dataset_fingerprint':{'$ne':fingerprint}}) for name in ['businesses','reviews']):
            raise ValueError('Target contains another fingerprint; no automatic deletion/mixing')
        ensure_indexes(db)
        db.ingestion_runs.update_one({'_id':run_id},{'$set':{'kind':'yelp_load','status':'running','selection':selection}},upsert=True)
        cutoff=datetime.fromisoformat(analysis['dataset_cutoff']);selected=[];batch=[];counts=Counter()
        tokens=analysis['scope_definition']['families'][selection['family']]
        for document,families,n,first,last in stage.execute('SELECT b.doc,b.families,COALESCE(l.n,0),l.first,l.last FROM businesses b LEFT JOIN life l ON b.id=l.id WHERE b.area=?',(selection['area_id'],)):
            if selection['family'] not in json.loads(families): continue
            row=json.loads(document);selected.append(row['business_id'])
            primary=next(token for token in tokens if token in row['categories'])
            batch.append(business_document(row,datetime.fromisoformat(first) if first else None,datetime.fromisoformat(last) if last else None,n,cutoff,fingerprint,primary))
            if len(batch)==500: counts.update(write_batch(db.businesses,batch));batch.clear()
        counts.update(write_batch(db.businesses,batch))
        assert len(selected)==choice['businesses']
        selected_path=directory/f'selected-{run_id}.json';atomic_json(selected_path,selected)
        totals=Counter()
        with Client('tcp://dask-scheduler:8786') as client:
            client.wait_for_workers(2,timeout=60);workers=sorted(client.scheduler_info()['workers'])
            for offset in range(0,len(analysis['review_parts']),2):
                futures=[client.compute(delayed(load_partition)(part['path'],str(selected_path),fingerprint),workers=[workers[i]],allow_other_workers=False,retries=0) for i,part in enumerate(analysis['review_parts'][offset:offset+2])]
                for result in client.gather(futures): totals.update(result['counts'])
                for future in futures: future.release()
                print(json.dumps({'phase':'approved_load','completed_partitions':min(offset+2,len(analysis['review_parts']))}),flush=True)
        proof={'businesses':db.businesses.count_documents({'dataset_fingerprint':fingerprint}),'reviews':db.reviews.count_documents({'dataset_fingerprint':fingerprint})}
        assert proof['businesses']==choice['businesses']
        assert proof['reviews']==choice['unique_clean_reviews']==totals['selected']
        result={'status':'complete','verification':proof,'business_writes':dict(counts),'review_writes':dict(totals),'selection':selection}
        db.ingestion_runs.update_one({'_id':run_id},{'$set':result})
        atomic_json(directory/'load-report.json',result)
        return result
    finally: stage.close();db.client.close()
