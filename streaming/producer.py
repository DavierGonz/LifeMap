"""Reproduce reseñas reales posteriores a T, sin modificar MongoDB ni E1."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import time


def instant(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def validate_model(manifest, cutoff, model_root):
    """Falla antes de publicar si el artefacto no tiene procedencia temporal."""
    cutoff = instant(cutoff)
    if manifest.get('temporal_training_verified') is not True:
        raise ValueError('Model lacks verified temporal training; E1 model is not eligible')
    if instant(manifest['training_max_date']) > cutoff:
        raise ValueError('Training includes reviews after cutoff')
    if instant(manifest['selection_max_date']) > cutoff:
        raise ValueError('Model selection includes reviews after cutoff')
    root = Path(model_root).resolve()
    predictor = (root / manifest['predictor']).resolve()
    if not predictor.is_relative_to(root):
        raise ValueError('Predictor must stay in model volume')
    if hashlib.sha256(predictor.read_bytes()).hexdigest() != manifest['sha256']:
        raise ValueError('Model checksum mismatch')
    if not manifest.get('model_id') or not re.fullmatch('[0-9a-f]{64}', manifest.get('training_review_ids_sha256', '')):
        raise ValueError('Training review provenance is required')
    return manifest['model_id']


def publish(rows, sender, cutoff, seconds_per_day=10 / 30, limit=None,
            clock=time.monotonic, sleep=time.sleep, model_id=None, run_id=None):
    """Orden de envio global; Kafka garantiza orden solo dentro de particion."""
    if not math.isfinite(seconds_per_day) or seconds_per_day < 0:
        raise ValueError('seconds_per_day must be finite and nonnegative')
    if limit is not None and limit <= 0:
        raise ValueError('limit must be positive')
    cutoff = instant(cutoff)
    first = previous = None
    start = clock()
    origin = None
    counts = {}
    sent = 0
    for row in rows:
        date = instant(row['date'])
        order = (date, row['review_id'])
        if date <= cutoff:
            raise ValueError('Review is not strictly after cutoff')
        if previous is not None and order <= previous:
            raise ValueError('Source must be strictly sorted by date, review_id')
        if not row.get('business_id') or not row.get('text', '').strip():
            raise ValueError('Missing business key or review text')
        if first is None:
            first = date
            origin = clock()
        due = (date - first).total_seconds() / 86400 * seconds_per_day
        delay = due - (clock() - origin)
        if delay > 0:
            sleep(delay)
        # No estrellas ni sentimientos previamente calculados: solo datos del evento.
        event = {k: row[k] for k in ('review_id', 'business_id', 'text')}
        event.update(date=date.isoformat(), cutoff=cutoff.isoformat(),
                     schema_version=1, model_id=model_id, replay_id=run_id,
                     sequence=sent, emitted_at=datetime.now(timezone.utc).isoformat())
        metadata = sender(row['business_id'].encode('utf-8'), event,
                          int(date.timestamp() * 1000))
        counts[str(metadata.partition)] = counts.get(str(metadata.partition), 0) + 1
        previous = order
        sent += 1
        if limit is not None and sent >= limit:
            break
    return {'sent': sent, 'partitions': counts, 'first_date': first.isoformat() if first else None,
            'last_date': previous[0].isoformat() if previous else None,
            'seconds': round(clock() - start, 3), 'model_id': model_id, 'replay_id': run_id}


def reader():
    from pymongo import MongoClient
    password = Path('/run/secrets/mongo_reader_password').read_text().strip()
    return MongoClient('mongodb', username='lifemap_reader', password=password,
                       authSource='lifemap', maxPoolSize=2, serverSelectionTimeoutMS=10000)


def ensure_topic(bootstrap):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id='lifemap-topic')
    try:
        try:
            admin.create_topics([NewTopic('reviews', num_partitions=3, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
        description = admin.describe_topics(['reviews'])[0]
        if description.get('error_code') or len(description['partitions']) < 3:
            raise ValueError('reviews must have at least three healthy partitions')
        return len(description['partitions'])
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cutoff', required=True, help='UTC ISO date, exclusive lower bound')
    parser.add_argument('--manifest', default='/models/sentiment-temporal.json')
    parser.add_argument('--model-root', default='/models')
    parser.add_argument('--bootstrap', default='kafka:9092')
    parser.add_argument('--seconds-per-day', type=float, default=10 / 30)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--after', help='UTC lower bound for demo replay, or latest received event; model cutoff stays unchanged')
    parser.add_argument('--run-id', required=True, help='Identifica una reproduccion; repetirlo no deduplica Kafka')
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    model_id = validate_model(manifest, args.cutoff, args.model_root)
    ensure_topic(args.bootstrap)
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks='all',
                             enable_idempotence=True, max_in_flight_requests_per_connection=1,
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))
    client = reader()
    try:
        lower=instant(args.cutoff)
        if args.after=='latest':
            latest=client.lifemap.stream_reviews.find_one(sort=[('date',-1)])
            if latest:lower=max(lower,instant(latest['date']))
        elif args.after:
            lower=instant(args.after)
            if lower<instant(args.cutoff):raise ValueError('after must not precede the immutable model cutoff')
        # External Mongo sort may spill to disk; no index/data mutation is necessary.
        rows = (client.lifemap.reviews.find({'date': {'$gt': lower}},
                 {'review_id': 1, 'business_id': 1, 'text': 1, 'date': 1, '_id': 0})
                .sort([('date', 1), ('review_id', 1)]).allow_disk_use(True).batch_size(100))
        def send(key, event, timestamp):
            return producer.send('reviews', key=key, value=event, timestamp_ms=timestamp).get(timeout=30)
        print(json.dumps(publish(rows, send, args.cutoff, args.seconds_per_day,
                                 args.limit, model_id=model_id, run_id=args.run_id)))
    finally:
        producer.close(timeout=10)
        client.close()


if __name__ == '__main__':
    main()
