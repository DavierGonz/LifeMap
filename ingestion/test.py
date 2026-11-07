"""Prueba real pequena: dos workers, deduplicacion global y MongoDB propio."""
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest

def run():
    suite=unittest.defaultTestLoader.discover('/app/tests',pattern='test_yelp_ingestion.py')
    if not unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful():
        raise RuntimeError('Ingestion unit tests failed')
    from distributed import Client
    from ingestion.download import atomic_json
    from ingestion.etl import process_partition
    from ingestion.run import connect_stage, initialize_stage, deduplicate_reviews
    from storage.yelp import database, business_document, review_document, write_batch, ensure_indexes
    with tempfile.TemporaryDirectory(prefix='yelp-fixture-',dir='/data') as temporary:
        path=Path(temporary);lookup=path/'lookup.json'
        atomic_json(lookup,{'raw_ids':['b1','bad-geo'],'clean_ids':['b1']})
        def row(review_id,date='2020-01-01 00:00:00',business_id='b1',text='synthetic fixture'):
            return json.dumps({'review_id':review_id,'business_id':business_id,'user_id':'fixture-user','text':text,'date':date,'stars':4.0,'useful':0,'funny':0,'cool':0})
        inputs=[[row('r1'),row('r2','2020-01-03 00:00:00'),row('orphan',business_id='absent')],[row('r1'),row('empty',text=' '),row('removed',business_id='bad-geo')]]
        with Client('tcp://dask-scheduler:8786') as client:
            client.wait_for_workers(2,timeout=30);workers=sorted(client.scheduler_info()['workers'])
            futures=[client.submit(process_partition,lines,'review',i,temporary,str(lookup),workers=[workers[i]],allow_other_workers=False,pure=False,retries=0) for i,lines in enumerate(inputs)]
            results=client.gather(futures)
        counts=Counter();stage=connect_stage(path/'fixture.sqlite');initialize_stage(stage)
        for i,result in enumerate(results):
            counts.update(result['counts'])
            counts.update(deduplicate_reviews(stage,result['path'],path/f'canonical-{i}.parquet'))
        assert counts['read']==6 and counts['duplicates']==1 and counts['unique']==2
        assert counts['orphan_business_id']==counts['business_removed_by_cleaning']==counts['empty_text']==1
        life=stage.execute('SELECT n,first,last FROM life WHERE id=?',('b1',)).fetchone()
        assert life==(2,'2020-01-01 00:00:00','2020-01-03 00:00:00')
        stage.close()
    db=database()
    try:
        ensure_indexes(db)
        definitive_before={'businesses':db.businesses.count_documents({}),'reviews':db.reviews.count_documents({})}
        businesses='__yelp_ingestion_test_businesses';reviews='__yelp_ingestion_test_reviews'
        ensure_indexes(db,businesses,reviews)
        record=business_document({'business_id':'__lifemap_yelp_fixture_b1','name':'Synthetic LifeMap ingestion fixture','latitude':6.2,'longitude':-75.5,'is_open':1,'categories':['Fixture']},datetime(2020,1,1),datetime(2020,1,3),2,datetime(2020,1,5),'synthetic-fixture-v1')
        first=write_batch(db[businesses],[record]);second=write_batch(db[businesses],[record])
        assert second['upserted']==0 and second['matched']==1 and second['modified']==0
        review=review_document({'review_id':'__lifemap_yelp_fixture_r1','business_id':record['business_id'],'date':'2020-01-01 00:00:00','text':'Synthetic fixture only','stars':4.0},'synthetic-fixture-v1')
        write_batch(db[reviews],[review]);repeat=write_batch(db[reviews],[review])
        assert repeat['upserted']==0 and repeat['modified']==0
        assert db[businesses].find_one({'_id':record['business_id']})['business_days']==2
        assert db[reviews].find_one({'_id':review['_id']})['business_id']==record['business_id']
        # Ejecutar la misma funcion de lotes definitivos, aislada en fixtures,
        # una vez por worker para comprobar credenciales y repeticion.
        import pyarrow as pa
        import pyarrow.parquet as pq
        from ingestion.etl import review_schema
        from ingestion.load import load_partition
        with tempfile.TemporaryDirectory(prefix='yelp-load-fixture-',dir='/data') as temporary:
            path=Path(temporary);selection=path/'selected.json';part=path/'reviews.parquet'
            atomic_json(selection,[record['business_id']])
            row={key:value for key,value in review.items() if key not in {'_id','dataset_fingerprint'}}
            row['date']=row['date'].isoformat(sep=' ')
            pq.write_table(pa.Table.from_pylist([row],schema=review_schema()),part)
            with Client('tcp://dask-scheduler:8786') as client:
                workers=sorted(client.scheduler_info()['workers'])
                for worker in workers:
                    loaded=client.submit(load_partition,str(part),str(selection),'synthetic-fixture-v1',reviews,workers=[worker],allow_other_workers=False,pure=False).result(timeout=30)
                    assert loaded['counts']['selected']==1 and loaded['counts']['upserted']==0
                assert loaded['counts']['modified']==0
        assert db[businesses].count_documents({'location':{'$geoWithin':{'$centerSphere':[[-75.5,6.2],1/6378.1]}}})==1
        definitive_after={'businesses':db.businesses.count_documents({}),'reviews':db.reviews.count_documents({})}
        assert definitive_before==definitive_after
        report={'status':'passed','distributed_workers':[r['worker'] for r in results],'fixture_counts':dict(counts),'life':list(life),'mongodb':{'geojson_index_and_query':True,'idempotent_business_and_review_batches':True,'distributed_writer_batches_both_workers':True,'definitive_counts':definitive_after,'fixture_collections':[businesses,reviews]}}
        atomic_json('/data/yelp-ingestion-tests.json',report);print(json.dumps(report,indent=2))
    finally: db.client.close()

if __name__=='__main__': run()
