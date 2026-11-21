"""Publica y vuelve a leer 100 reseñas reales; no clasifica ni escribe en MongoDB."""
import argparse
import json
from pathlib import Path
import time
import uuid

from streaming.producer import ensure_topic, instant, publish, reader, validate_model


def main():
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cutoff', required=True)
    parser.add_argument('--manifest', default='/models/sentiment-temporal.json')
    parser.add_argument('--output', default='/data/lifemap-e2/producer-smoke.json')
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    model_id = validate_model(manifest, args.cutoff, '/models')
    n = ensure_topic('kafka:9092')
    consumer = KafkaConsumer(bootstrap_servers='kafka:9092', enable_auto_commit=False,
                             group_id=None, value_deserializer=lambda b: json.loads(b))
    partitions = [TopicPartition('reviews', p) for p in range(n)]
    consumer.assign(partitions)
    start_offsets = consumer.end_offsets(partitions)
    for p, offset in start_offsets.items():
        consumer.seek(p, offset)
    producer = KafkaProducer(bootstrap_servers='kafka:9092', enable_idempotence=True,
                             acks='all', max_in_flight_requests_per_connection=1,
                             value_serializer=lambda v: json.dumps(v).encode())
    db = reader()
    replay = 'phase1-' + uuid.uuid4().hex
    expected = []
    try:
        rows = db.lifemap.reviews.find({'date': {'$gt': instant(args.cutoff)}},
                  {'_id': 0, 'review_id': 1, 'business_id': 1, 'date': 1, 'text': 1})
        rows = rows.sort([('date', 1), ('review_id', 1)]).allow_disk_use(True).limit(100)
        def send(key, value, timestamp):
            result = producer.send('reviews', key=key, value=value, timestamp_ms=timestamp).get(timeout=30)
            expected.append((result.partition, result.offset, key, value['review_id'], value['date']))
            return result
        result = publish(rows, send, args.cutoff, 0, 100, model_id=model_id, run_id=replay)
        assert result['sent'] == 100
        received = []
        deadline = time.monotonic() + 45
        while len(received) < 100 and time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000).values():
                for r in records:
                    if r.value.get('replay_id') != replay:
                        continue
                    assert r.key.decode() == r.value['business_id']
                    assert instant(r.value['date']) > instant(args.cutoff)
                    assert r.timestamp == int(instant(r.value['date']).timestamp() * 1000)
                    assert r.value['model_id'] == model_id
                    assert 'stars' not in r.value and 'sentiment' not in r.value
                    received.append((r.partition, r.offset, r.key, r.value['review_id'], r.value['date']))
        assert sorted(received) == sorted(expected), 'Kafka readback differs from acknowledged writes'
        for p in partitions:
            ordered = [r for r in received if r[0] == p.partition]
            assert ordered == sorted(ordered, key=lambda r: r[1])
            assert [r[4] for r in ordered] == sorted(r[4] for r in ordered)
        business_partitions = {}
        for partition, offset, key, review_id, date in received:
            assert key not in business_partitions or business_partitions[key] == partition
            business_partitions[key] = partition
        report = {**result, 'topic': 'reviews', 'topic_partitions': n,
                  'readback': len(received), 'key_consistent': True, 'ordered_per_partition': True,
                  'training_temporally_excluded': True, 'model_sha256': manifest['sha256'],
                  'global_consumer_order_guaranteed': False,
                  'mongo_writes': 0, 'streaming_inference_test': False}
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2))
        print(json.dumps(report))
    finally:
        producer.close(timeout=10)
        consumer.close()
        db.close()


if __name__ == '__main__':
    main()
