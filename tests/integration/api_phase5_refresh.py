"""Una reseña Yelp nueva en el flujo para comprobar actualización API/Power BI."""
import argparse
import json
from pathlib import Path
from kafka import KafkaProducer
from streaming.sinks import writer
from streaming.producer import instant
from ml.risk import load_risk


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','publish','verify']);args=p.parse_args()
    db=writer();cache=Path('/data/lifemap-e2/phase5-refresh.json')
    try:
        if args.action=='prepare' and not cache.exists():
            latest=db.stream_reviews.find_one(sort=[('date',-1)])['date']
            candidates=[]
            for state in db.risk_features.find({}):
                lon,lat=state['geometry']['coordinates']
                if not -75.24<=lon<=-75.09 or not 39.90<=lat<=40.02:continue
                row=db.reviews.find_one({'business_id':state['business_id'],'date':{'$gt':max(latest,state['as_of'])}},sort=[('date',1),('_id',1)])
                if row:candidates.append(row)
            if not candidates:raise ValueError('No new existing Yelp review for refresh check')
            row=min(candidates,key=lambda r:(r['date'],r['_id']))
            cache.write_text(json.dumps({'review_id':row['_id'],'business_id':row['business_id'],'date':instant(row['date']).isoformat()}))
        selected=json.loads(cache.read_text());row=db.reviews.find_one({'_id':selected['review_id']})
        assert row and row['business_id']==selected['business_id']
        if args.action=='publish':
            model,_=load_risk(config_path='/app/config/risk.json')
            event={k:row[k] for k in ['business_id','text']}
            event.update(review_id=row['_id'],date=instant(row['date']).isoformat(),model_id=model['sentiment_model_id'],
                         replay_id='production',schema_version=1)
            producer=KafkaProducer(bootstrap_servers='kafka:9092',enable_idempotence=True,acks='all',
                                  value_serializer=lambda r:json.dumps(r).encode())
            try:
                sent=producer.send('reviews',key=row['business_id'].encode(),value=event,
                                  timestamp_ms=int(instant(row['date']).timestamp()*1000)).get(timeout=30)
                print(json.dumps(dict(selected,partition=sent.partition,end_offset=sent.offset+1)))
            finally:producer.close()
        else:
            state=db.risk_features.find_one({'_id':selected['business_id']})
            stream=db.stream_reviews.find_one({'_id':selected['review_id']})
            print(json.dumps(dict(selected,processed=bool(stream and stream.get('risk_processed_models')),
                                 current_as_of=instant(state['as_of']).isoformat(),
                                 businesses=db.businesses.count_documents({}),reviews=db.reviews.count_documents({}),
                                 survival_results=db.survival_results.count_documents({}))))
    finally:db.client.close()


if __name__=='__main__':main()
