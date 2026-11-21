"""CI real: modelos inmutables + 100 eventos Yelp por Kafka/Spark/Mongo.

La selección se conserva entre builds: un segundo envío verifica idempotencia,
no inventa textos ni carga Yelp. Los resultados de fixtures se excluyen.
"""
import argparse
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import time

from kafka import KafkaProducer,KafkaConsumer,TopicPartition
from kafka.partitioner.default import DefaultPartitioner
from ml.inference import predict
from ml.risk import load_risk,probability,FEATURES
from streaming.producer import instant,validate_model,publish
from streaming.sinks import writer,pipeline_hash,hotspot,collections
from streaming.risk import risk_names

ROOT=Path(__file__).resolve().parents[1]
CACHE=Path('/data/lifemap-e2/ci-smoke-100.json')


def assets():
    cfg=json.loads((ROOT/'config/streaming.json').read_text())
    manifest=json.loads(Path(cfg['temporal_manifest']).read_text())
    assert validate_model(manifest,cfg['cutoff'],'/models')==cfg['model_id']
    assert manifest['sha256']==cfg['model_sha256']
    assert pipeline_hash(Path('/models')/manifest['spark_pipeline'])==cfg['spark_pipeline_sha256']
    lock=json.loads(Path('/checkpoints/production/model-lock.json').read_text())
    assert lock['model_id']==cfg['model_id'] and lock['predictor_sha256']==cfg['model_sha256']
    assert lock['spark_pipeline_sha256']==cfg['spark_pipeline_sha256']
    risk,risk_manifest=load_risk(config_path=str(ROOT/'config/risk.json'))
    assert pipeline_hash(Path('/models')/risk_manifest['spark_pipeline'])==risk_manifest['spark_pipeline_sha256']
    state=json.loads(Path('/checkpoints/production/status.json').read_text())
    assert state['active'] and time.time()-state['updated_epoch']<120
    assert state['model_id']==cfg['model_id'] and state['risk_model_id']==risk['model_id']
    risk_lock=json.loads(Path('/checkpoints/production/risk/model-lock.json').read_text())
    assert risk_lock['sha256']==risk_manifest['sha256']
    return cfg,manifest,risk,risk_manifest


def counts(db):return {name:db.command('collStats',name)['count'] for name in ['businesses','reviews']}


def verify(db,ids,cfg,manifest,risk,scope='production'):
    source=collections(scope)[0];_,alert_collection=risk_names(scope)
    rows=list(db[source].find({'_id':{'$in':ids}}))
    if len(rows)!=100 or any(risk['model_id'] not in r.get('risk_processed_models',[]) for r in rows):return None
    predictor=json.loads((Path('/models')/manifest['predictor']).read_text());labels=Counter()
    for r in rows:
        assert r['test_fixture']==(scope!='production') and r['model_id']==cfg['model_id'] and r['model_sha256']==cfg['model_sha256']
        assert instant(r['date'])>instant(cfg['cutoff'])
        expected=predict(r['text'],predictor)
        assert r['prediction']==expected['label']
        assert max(abs(r['probabilities'][k]-v) for k,v in expected['probabilities'].items())<1e-6
        assert r['location']['type']=='Point'
        assert r['kafka']['partition']==DefaultPartitioner()(r['business_id'].encode(),[0,1,2],[0,1,2])
        labels[r['sentiment']]+=1
    for part in range(3):
        ordered=sorted((r for r in rows if r['kafka']['partition']==part),key=lambda r:r['kafka']['offset'])
        assert [r['date'] for r in ordered]==sorted(r['date'] for r in ordered),'Per-partition chronological sample order violated'
    windows=list(db.stream_windows.find({}));alerts=list(db[alert_collection].find({}))
    assert windows and alerts
    for w in windows:
        assert w['geometry']['type']=='Polygon' and 0<=w['negative']<=w['reviews'] and w['finalized']
        previous=db.stream_windows.find_one({'_id':w.get('previous_window_id')}) if w.get('previous_window_id') else None
        flag,change=hotspot(w,previous,cfg['hotspot_min_reviews'],cfg['hotspot_increase_percentage_points'])
        assert flag==w['is_hotspot']
    for a in alerts:
        assert a['test_fixture']==(scope!='production') and a['model_id']==risk['model_id']
        assert set(a['features'])==set(FEATURES) and abs(a['probability']-probability(a['features'],risk))<1e-8
        assert a['probability']>=risk['threshold'] and a['geometry']['type']=='Point'
    assert counts(db)==dict(businesses=16944,reviews=1320594)
    return dict(classified=100,predictions=dict(labels),risk_processed=100,windows=len(windows),alerts=len(alerts),
                active_alerts=sum(a['active'] for a in alerts),hotspots=db.hotspots.count_documents({}),
                model_id=cfg['model_id'],sentiment_sha256=cfg['model_sha256'],risk_model_id=risk['model_id'],
                risk_score_calibrated=False,real_events=True,synthetic_texts=False,scope=scope,isolated_test_scope=scope!='production',immutable_e1_counts=counts(db),
                fingerprints={r['review_id']:str(r['processed_at']) for r in rows})


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['models','smoke','publish-test','verify-test']);p.add_argument('--seconds-per-day',type=float,default=0);p.add_argument('--scope',default='production');a=p.parse_args()
    collections(a.scope)
    if a.action.endswith('-test'):assert a.scope.startswith('test_')
    cfg,manifest,risk,risk_manifest=assets()
    if a.action=='models':
        print(json.dumps(dict(verified=True,sentiment=cfg['model_id'],sha256=cfg['model_sha256'],risk=risk['model_id'],cutoff=cfg['cutoff'],loaded_checkpoint_models_match=True)));return
    db=writer()
    try:
        assert counts(db)==dict(businesses=16944,reviews=1320594)
        if not CACHE.exists():
            latest=db.stream_reviews.find_one(sort=[('date',-1)])['date']
            rows=list(db.reviews.find({'date':{'$gt':latest}},{'_id':1,'business_id':1,'date':1}).sort([('date',1),('_id',1)]).limit(100))
            assert len(rows)==100
            CACHE.parent.mkdir(parents=True,exist_ok=True)
            CACHE.write_text(json.dumps(dict(model_id=cfg['model_id'],ids=[r['_id'] for r in rows])))
        selection=json.loads(CACHE.read_text());assert selection['model_id']==cfg['model_id']
        ids=selection['ids'];assert len(set(ids))==100
        if a.action=='verify-test':
            result=verify(db,ids,cfg,manifest,risk,a.scope);assert result
            result.pop('fingerprints');print(json.dumps(result));return
        rows=list(db.reviews.find({'_id':{'$in':ids}}).sort([('date',1),('_id',1)]))
        assert len(rows)==100
        for r in rows:r['review_id']=r['_id']
        before=verify(db,ids,cfg,manifest,risk) if a.action=='smoke' else None
        producer=KafkaProducer(bootstrap_servers='kafka:9092',enable_idempotence=True,acks='all',max_in_flight_requests_per_connection=1,
                               value_serializer=lambda value:json.dumps(value).encode())
        starts=None
        if a.action=='publish-test':
            consumer=KafkaConsumer(bootstrap_servers='kafka:9092',group_id=None,enable_auto_commit=False,
                                   request_timeout_ms=3000,session_timeout_ms=2000,heartbeat_interval_ms=500)
            try:starts={'reviews':{str(p.partition):o for p,o in consumer.end_offsets([TopicPartition('reviews',p) for p in range(3)]).items()}}
            finally:consumer.close()
        offsets={};partitions=Counter()
        def send(key,event,timestamp):
            if a.action=='publish-test':event.update(test_fixture=True,replay_id=a.scope)
            sent=producer.send('reviews',key=key,value=event,timestamp_ms=timestamp).get(timeout=30)
            offsets[str(sent.partition)]=max(offsets.get(str(sent.partition),0),sent.offset+1);partitions[sent.partition]+=1
            return sent
        try:published=publish(rows,send,cfg['cutoff'],a.seconds_per_day,limit=100,model_id=cfg['model_id'],run_id='production')
        finally:producer.close()
        assert len(partitions)==3
        if a.action=='publish-test':print(json.dumps(dict(sent=100,scope=a.scope,real_yelp=True,synthetic_texts=False,end_offsets=offsets,starting_offsets=starts)));return
        for _ in range(80):
            result=verify(db,ids,cfg,manifest,risk)
            status=json.loads(Path('/checkpoints/production/status.json').read_text())
            progress=[q['sources'][0]['endOffset']['reviews'] for q in status['progress'].values() if q]
            if result and len(progress)==2 and all(all(q.get(part,-1)>=end for part,end in offsets.items()) for q in progress):break
            time.sleep(3)
        else:raise RuntimeError('100 real Kafka events did not reach both Spark sinks')
        if before:assert before['fingerprints']==result['fingerprints'],'Duplicate replay changed existing persisted reviews'
        result.pop('fingerprints');result.update(published_messages=100,partitions=dict(partitions),chronological_send=True,
                     key='business_id',cached_selection=True,replayed=before is not None,idempotent_replay_verified=bool(before),
                     sampling='100 chronological real Yelp events; directed F4 alert sample is not population prevalence',
                     kafka_end_offsets=offsets,published=published,verified_utc=datetime.now(timezone.utc).isoformat())
        print(json.dumps(result))
    finally:db.client.close()


if __name__=='__main__':main()
