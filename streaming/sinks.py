"""Sinks Mongo idempotentes. Nunca escriben en las colecciones E1."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re

from analytics.geo import geometry


def collections(scope):
    if scope == 'production':
        return 'stream_reviews', 'stream_windows', 'hotspots'
    if not re.fullmatch(r'test_[a-z0-9_]{1,40}', scope):
        raise ValueError('Only production or an isolated test_ scope is allowed')
    return tuple('__e2_' + scope + '_' + x for x in ('reviews', 'windows', 'hotspots'))


def writer():
    from pymongo import MongoClient
    password = Path('/run/secrets/mongo_writer_password').read_text().strip()
    return MongoClient('mongodb', username='lifemap_writer', password=password,
                       authSource='lifemap', maxPoolSize=2, serverSelectionTimeoutMS=10000).lifemap


def pipeline_hash(path):
    root = Path(path).resolve()
    digest = hashlib.sha256()
    for p in sorted(root.rglob('*')):
        if p.is_file():
            digest.update(p.relative_to(root).as_posix().encode() + b'\0')
            digest.update(p.read_bytes())
    if not (root/'metadata/_SUCCESS').exists():
        raise ValueError('Incomplete saved Spark model')
    return digest.hexdigest()


def bind_checkpoint(directory, binding):
    path = Path(directory)/'model-lock.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if json.loads(path.read_text()) != binding:
            raise ValueError('Checkpoint model/configuration differs; use a new checkpoint, never reset offsets')
    else:
        # Exclusive creation also prevents silently rebinding an existing model lock.
        with path.open('x') as f:
            json.dump(binding, f, sort_keys=True)


def hotspot(current, previous, minimum=30, threshold=15):
    if current['reviews'] < minimum or previous is None or previous['reviews'] < minimum:
        return False, None
    change = 100 * (current['negative']/current['reviews'] - previous['negative']/previous['reviews'])
    return change >= threshold, change


def ensure_indexes(db, scope):
    reviews, windows, hotspots = collections(scope)
    db[reviews].create_index([('location', '2dsphere')], name='location_2dsphere')
    db[reviews].create_index([('geohash', 1), ('date', 1)], name='cell_date')
    for name in (windows, hotspots):
        db[name].create_index([('geometry', '2dsphere')], name='geometry_2dsphere')
        db[name].create_index([('geohash', 1), ('start', 1)], unique=True, name='cell_window')


def save_reviews(frame, batch_id, scope, manifest, checkpoint, simulate_failure=False, risk_assets=None):
    from pymongo import UpdateOne
    db = writer()
    try:
        name = collections(scope)[0]
        operations = []
        received = []
        processed = 0
        for row in frame.toLocalIterator():
            processed += 1
            r = row.asDict(recursive=True)
            received.append(r)
            doc = {k: r[k] for k in ('review_id', 'business_id', 'text', 'date', 'date_original', 'geohash')}
            doc.update(date=r['date'].replace(tzinfo=timezone.utc),
                       location={'type': 'Point', 'coordinates': [r['longitude'], r['latitude']]},
                       prediction=int(r['prediction']),
                       sentiment=['negative', 'neutral', 'positive'][int(r['prediction'])],
                       probabilities=dict(zip(['negative', 'neutral', 'positive'], r['probabilities'])),
                       model_id=manifest['model_id'], model_sha256=manifest['sha256'],
                       processed_at=datetime.now(timezone.utc), first_batch_id=int(batch_id),
                       kafka={'topic': r['topic'], 'partition': r['partition'], 'offset': r['offset']},
                       test_fixture=scope != 'production')
            operations.append(UpdateOne({'_id': r['review_id']}, {'$setOnInsert': doc}, upsert=True))
            if len(operations) == 100:
                db[name].bulk_write(operations, ordered=False)
                operations.clear()
        if operations:
            db[name].bulk_write(operations, ordered=False)
        if risk_assets is not None:
            from streaming.risk import update_risk
            update_risk(db, received, scope, risk_assets, frame.sparkSession)
        marker = Path(checkpoint)/'injected-sink-failure'
        if processed and simulate_failure and not marker.exists():
            if scope == 'production':
                raise ValueError('Fault injection is forbidden for production')
            marker.write_text('Intentional test failure after Mongo write and before offset commit')
            raise RuntimeError('Intentional test sink failure; retry must not duplicate Mongo documents')
    finally:
        db.client.close()


def window_id(cell, start):
    return cell + ':' + start.isoformat()


def save_windows(frame, batch_id, scope, manifest, config):
    db = writer()
    try:
        _, window_name, hotspot_name = collections(scope)
        touched = []
        for row in frame.toLocalIterator():
            start = row.window.start
            doc = dict(geohash=row.geohash, start=start, end=row.window.end,
                       reviews=int(row.reviews), negative=int(row.negative),
                       negative_percentage=100*row.negative/row.reviews,
                       geometry=geometry(row.geohash), finalized=True,
                       model_id=manifest['model_id'], model_sha256=manifest['sha256'])
            id = window_id(row.geohash, start)
            db[window_name].update_one({'_id': id}, {'$set': doc, '$setOnInsert': {
                'processed_at': datetime.now(timezone.utc), 'first_batch_id': int(batch_id)}}, upsert=True)
            touched.extend([(row.geohash, start), (row.geohash, start + timedelta(days=30))])
        # Evaluate after all upserts, and also revisit next windows: no cursor/order assumption.
        for cell, start in set(touched):
            current = db[window_name].find_one({'_id': window_id(cell, start)})
            if not current:
                continue
            previous = db[window_name].find_one({'_id': window_id(cell, start-timedelta(days=30))})
            flag, change = hotspot(current, previous, config['hotspot_min_reviews'],
                                   config['hotspot_increase_percentage_points'])
            fields = dict(is_hotspot=flag, change_percentage_points=change,
                          previous_window_id=previous['_id'] if previous else None,
                          minimum_reviews=config['hotspot_min_reviews'],
                          threshold_percentage_points=config['hotspot_increase_percentage_points'])
            db[window_name].update_one({'_id': current['_id']}, {'$set': fields})
            if flag:
                doc = {**current, **fields}
                id = doc.pop('_id')
                db[hotspot_name].replace_one({'_id': id}, {'_id': id, **doc}, upsert=True)
    finally:
        db.client.close()
