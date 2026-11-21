"""Publica Yelp real y audita alertas reales; no selecciona por etiqueta futura."""
import argparse
import hashlib
import json
from pathlib import Path

from ml.risk import load_risk, FEATURES, probability, feature_values, competitor_density
from streaming.producer import instant
from streaming.risk import risk_names
from streaming.sinks import writer, collections


def main():
    from kafka import KafkaProducer
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','publish','verify'])
    p.add_argument('--scope',default='production');args=p.parse_args();collections(args.scope)
    model,manifest=load_risk(config_path='/app/config/risk.json')
    root=Path('/data/lifemap-e2')/model['model_id'];sample=root/'demo-real.json'
    db=writer();producer=None
    try:
        if args.action=='prepare':
            test=json.loads((root/'test-predictions.json').read_text());selected=[]
            if sample.exists():
                selected=json.loads(sample.read_text());held_out={r['business_id'] for r in test if r['probability']>=model['threshold']+.02}
                assert len(selected)==100 and all(r['business_id'] in held_out for r in selected)
                print(json.dumps({'selected':100,'businesses':len({r['business_id'] for r in selected}),
                    'selection':'held-out test businesses, high model score only; no outcome selection','sample_cache_hit':True,
                    'date_min':selected[0]['date'],'date_max':selected[-1]['date']}));return
            for r in sorted(test,key=lambda r:r['probability'],reverse=True):
                if r['probability']<model['threshold']+.02:continue
                raw=db.reviews.find_one({'business_id':r['business_id'],'date':{'$gt':instant(model['cutoff'])}},
                    {'_id':0,'review_id':1,'business_id':1,'date':1,'text':1},sort=[('date',1)])
                if not raw or db.stream_reviews.find_one({'_id':raw['review_id']}):continue
                raw['date']=instant(raw['date']).isoformat();selected.append(raw)
                if len(selected)==100:break
            assert len(selected)==100,'Need 100 held-out real reviews; never lower the fitted threshold for this test'
            selected.sort(key=lambda r:(r['date'],r['review_id']))
            sample.write_text(json.dumps(selected))
            print(json.dumps({'selected':100,'businesses':len({r['business_id'] for r in selected}),
                    'selection':'held-out test businesses, high model score only; no outcome selection',
                    'date_min':selected[0]['date'],'date_max':selected[-1]['date']}));return
        rows=json.loads(sample.read_text())
        if args.scope!='production':rows=rows[:20]
        ids=[r['review_id'] for r in rows]
        if args.action=='publish':
            producer=KafkaProducer(bootstrap_servers='kafka:9092',enable_idempotence=True,acks='all',
                                  max_in_flight_requests_per_connection=1,value_serializer=lambda r:json.dumps(r).encode())
            offsets={}
            for r in rows:
                event={**r,'model_id':model['sentiment_model_id'],'replay_id':args.scope,'schema_version':1}
                if args.scope!='production':event['test_fixture']=True
                sent=producer.send('reviews',key=r['business_id'].encode(),value=event,
                              timestamp_ms=int(instant(r['date']).timestamp()*1000)).get(timeout=30)
                offsets[str(sent.partition)]=max(offsets.get(str(sent.partition),0),sent.offset+1)
            print(json.dumps({'published':len(rows),'scope':args.scope,'end_offsets':offsets}));return
        source=collections(args.scope)[0];states,alerts=risk_names(args.scope)
        count=db[source].count_documents({'_id':{'$in':ids},'risk_processed_models':model['model_id']})
        found=list(db[alerts].find({'review_id':{'$in':ids},'model_id':model['model_id']}))
        for a in found:
            assert a['probability']>=model['threshold']
            assert set(a['features'])==set(FEATURES)
            assert a['model_sha256']==manifest['sha256'] and a['geometry']['type']=='Point'
            assert abs(a['probability']-probability(a['features'],model))<1e-8
            assert instant(a['event_date'])>instant(model['cutoff'])
            history=list(db[source].find({'business_id':a['business_id'],'date':{'$gt':instant(model['cutoff']),'$lte':a['event_date']},'stars':{'$exists':True}},
                         {'_id':0,'review_id':1,'date':1,'prediction':1,'stars':1}))
            baseline=db.risk_baselines.find_one({'_id':a['business_id']})
            catalog=db.risk_catalog.find_one({'_id':a['business_id']})
            density=competitor_density(db,catalog,a['event_date'],args.scope)
            recomputed,_=feature_values(baseline,history,a['event_date'],density,model['cutoff'])
            assert all(abs(recomputed[k]-a['features'][k])<1e-8 for k in FEATURES),'Stored feature depends on unavailable/future information'
        fingerprint=hashlib.sha256(json.dumps(sorted((a['_id'],str(a['processed_at']),a['probability']) for a in found)).encode()).hexdigest()
        offsets={}
        status=Path('/checkpoints')/args.scope/'status.json'
        if status.exists():
            q=json.loads(status.read_text())['progress'].get(args.scope+'_reviews')
            if q:
                for s in q.get('sources',[]):offsets.update(s['endOffset']['reviews'])
        print(json.dumps({'scope':args.scope,'expected':len(ids),'risk_processed':count,'alerts':len(found),'end_offsets':offsets,
              'active':sum(a['active'] for a in found),'alert_fingerprint':fingerprint,
              'alert_ids':[a['_id'] for a in found],'processed_at':{a['_id']:str(a['processed_at']) for a in found},
              'probability_range':[min(a['probability'] for a in found),max(a['probability'] for a in found)] if found else None,
              'states':db[states].count_documents({'business_id':{'$in':[r['business_id'] for r in rows]}})}))
    finally:
        if producer:producer.close(timeout=10)
        db.client.close()


if __name__=='__main__':main()
