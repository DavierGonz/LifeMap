"""Fixture acotado para Kafka real; ejecutar dentro de la imagen del productor."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from analytics.geo import geohash
from ml.inference import predict
from streaming.producer import instant, publish, validate_model
from streaming.sinks import collections, writer


def main():
    from kafka import KafkaProducer
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['real', 'initial', 'late', 'next', 'finish', 'too_late', 'verify'])
    p.add_argument('--scope', default='production')
    p.add_argument('--expected', type=int)
    args = p.parse_args()
    collections(args.scope)
    manifest = json.loads(Path('/models/sentiment-temporal.json').read_text())
    validate_model(manifest, '2019-01-01', '/models')
    predictor = json.loads((Path('/models')/manifest['predictor']).read_text())
    db = writer()
    producer = None
    try:
        if args.action == 'verify':
            name, window_name, hotspot_name = collections(args.scope)
            rows = list(db[name].find())
            if args.expected is not None:
                assert len(rows) == args.expected, (len(rows), args.expected)
            labels = {}
            for r in rows:
                assert r['model_id'] == manifest['model_id'] and r['model_sha256'] == manifest['sha256']
                assert r['date'] > instant('2019-01-01').replace(tzinfo=None)
                assert r['date'] == instant(r['date_original']).replace(tzinfo=None)
                expected = predict(r['text'], predictor)
                assert r['prediction'] == expected['label']
                assert max(abs(r['probabilities'][k]-v) for k,v in expected['probabilities'].items()) < 1e-6
                assert r['location']['type'] == 'Point'
                labels[r['sentiment']] = labels.get(r['sentiment'], 0) + 1
            windows = list(db[window_name].find({}, {'_id': 0}))
            hotspots = list(db[hotspot_name].find({}, {'_id': 0}))
            for w in windows:
                assert w['end']-w['start'] == timedelta(days=30)
                assert w['geometry']['type'] == 'Polygon' and w['finalized']
            polygon_matches = 0
            if windows:
                ring = windows[0]['geometry']['coordinates'][0]
                point = {'type': 'Point', 'coordinates': [(ring[0][0]+ring[2][0])/2, (ring[0][1]+ring[2][1])/2]}
                polygon_matches = db[window_name].count_documents({'geometry': {'$geoIntersects': {'$geometry': point}}})
                assert polygon_matches > 0
            first_doc = db[name].find_one({'_id': args.scope+'-a00'})
            result = dict(scope=args.scope, classified=len(rows), predictions=labels, windows=windows,
                          hotspots=hotspots, geo_query_matches=polygon_matches,
                          first_doc_processed_at=first_doc['processed_at'] if first_doc else None)
            status = Path('/checkpoints')/args.scope/'status.json'
            if status.exists():
                progress = json.loads(status.read_text())
                for query in progress['progress'].values():
                    for source in query.get('sources', []):
                        offsets = source['endOffset']
                        if isinstance(offsets, str): offsets = json.loads(offsets)
                        assert set(offsets['reviews']) == {'0', '1', '2'}, 'Kafka source must read all three partitions'
                result['kafka_partitions_verified'] = [0, 1, 2]
                recent = progress.get('recent', {}).get(args.scope+'_windows', [])
                result['dropped_by_watermark'] = sum(op.get('numRowsDroppedByWatermark', 0)
                    for batch in recent for op in batch.get('stateOperators', []))
                result['query_progress'] = progress['progress']
            print(json.dumps(result, default=str)); return
        producer = KafkaProducer(bootstrap_servers='kafka:9092', enable_idempotence=True,
                                 acks='all', max_in_flight_requests_per_connection=1,
                                 value_serializer=lambda d: json.dumps(d).encode())
        if args.action == 'real':
            rows = list(db.reviews.find({'date': {'$gt': instant('2019-01-01')}},
                        {'review_id': 1, 'business_id': 1, 'text': 1, 'date': 1, '_id': 0})
                        .sort([('date', 1), ('review_id', 1)]).allow_disk_use(True).limit(100))
            for boundary in ['2019-02-08', '2019-03-10', '2019-04-15']:
                r = db.reviews.find_one({'date': {'$gte': instant(boundary)}},
                    {'review_id': 1, 'business_id': 1, 'text': 1, 'date': 1, '_id': 0},
                    sort=[('date', 1), ('review_id', 1)], allow_disk_use=True)
                assert r;rows.append(r)
            def send(key, event, timestamp):
                return producer.send('reviews', key=key, value=event, timestamp_ms=timestamp).get(timeout=30)
            print(json.dumps(publish(rows, send, '2019-01-01', 0, model_id=manifest['model_id'], run_id='phase2-real')))
            return
        assert args.scope.startswith('test_'), 'Synthetic messages must use isolated test scope'
        business = db.businesses.find_one({}, sort=[('_id', 1)])
        positive = 'Excellent food and wonderful friendly service. I loved this restaurant.'
        negative = 'Terrible disgusting food and rude slow service. Never coming back.'
        assert predict(positive, predictor)['label'] == 2 and predict(negative, predictor)['label'] == 0
        events = []
        if args.action == 'initial':
            events = [(f'a{i:02}', '2019-01-20T12:00:00Z', negative if i<6 else positive) for i in range(29)]
            events.append(('advance1', '2019-02-02T12:00:00Z', positive))
        elif args.action == 'late':
            events = [('late_ok', '2019-01-29T12:00:00Z', positive), ('a00', '2019-01-20T12:00:00Z', negative)]
        elif args.action == 'next':
            events = [(f'b{i:02}', '2019-02-10T12:00:00Z', negative if i<21 else positive) for i in range(29)]
        elif args.action == 'finish':
            events = [('advance2', '2019-03-10T12:00:00Z', positive)]
        elif args.action == 'too_late':
            events = [('late_rejected', '2019-01-15T12:00:00Z', negative), ('a00', '2019-01-20T12:00:00Z', negative)]
        offsets = []
        for id, date, text in events:
            event = dict(review_id=args.scope+'-'+id, business_id=business['business_id'], text=text,
                         date=date, model_id=manifest['model_id'], schema_version=1,
                         replay_id=args.scope, test_fixture=True)
            m = producer.send('reviews', key=business['business_id'].encode(), value=event,
                              timestamp_ms=int(instant(date).timestamp()*1000)).get(timeout=30)
            offsets.append({'partition': m.partition, 'offset': m.offset})
        print(json.dumps({'scope': args.scope, 'action': args.action, 'sent': len(events), 'offsets': offsets}))
    finally:
        if producer: producer.close(timeout=10)
        db.client.close()


if __name__ == '__main__':
    main()
