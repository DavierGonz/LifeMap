"""Prueba local reproducible de Kafka/Spark/Mongo: real, retry, tardios y hotspots."""
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def compact_run(data):
    """Los documentos completos permanecen en Mongo; versionar solo evidencia util."""
    from collections import Counter
    counts = Counter()
    for w in data['windows']:
        counts[w['start']] += w['reviews']
    progress = {}
    partitions = set()
    for name, q in data.get('query_progress', {}).items():
        for source in q.get('sources', []):
            offsets = source['endOffset']
            offsets = json.loads(offsets) if isinstance(offsets, str) else offsets
            partitions.update(int(p) for p in offsets['reviews'])
        progress[name] = {'batch_id': q['batchId'], 'event_time': q.get('eventTime', {}),
                         'end_offsets': [s.get('endOffset') for s in q.get('sources', [])]}
    assert partitions == {0, 1, 2}
    result = {k: data[k] for k in ['scope', 'classified', 'predictions', 'geo_query_matches',
                                  'first_doc_processed_at', 'dropped_by_watermark'] if k in data}
    result.update(window_count=len(data['windows']), hotspot_count=len(data['hotspots']),
                  finalized_reviews=sum(counts.values()), reviews_by_window_start=dict(counts),
                  kafka_partitions_verified=sorted(partitions), query_progress=progress)
    if data['scope'] != 'production':
        result['windows'] = data['windows']
        result['hotspots'] = data['hotspots']
    return result


def main():
    if sys.version_info[:2] != (3, 10) or Path(sys.prefix).resolve() != (ROOT/'.venv').resolve():
        raise SystemExit('Use LifeMap .venv Python 3.10')
    scope = 'test_' + uuid.uuid4().hex[:12]
    output = ROOT/'work'/scope
    output.mkdir(parents=True)
    mounts = ['--mount', 'type=volume,src=lifemap_model_store,dst=/models,readonly',
              '--mount', 'type=volume,src=lifemap_stream_checkpoints,dst=/checkpoints,readonly',
              '--mount', f'type=bind,src={ROOT.as_posix()}/.secrets/mongo_writer_password.txt,dst=/run/secrets/mongo_writer_password,readonly']
    for src, target in [('streaming', 'streaming'), ('analytics', 'analytics'), ('ml', 'ml'), ('tests/integration', 'integration')]:
        mounts += ['--mount', f'type=bind,src={ROOT.as_posix()}/{src},dst=/app/{target},readonly']
    def helper(action, test_scope='production', expected=None):
        command = ['docker', 'run', '--rm', '--network', 'lifemap_backend', '--memory', '192m', '--cpus', '0.5',
                   *mounts, '--entrypoint', 'python', 'lifemap-producer:e2-v1', '-m', 'integration.streaming_phase2',
                   action, '--scope', test_scope]
        if expected is not None: command += ['--expected', str(expected)]
        r = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=120)
        if r.returncode:
            print(r.stderr[-2500:], flush=True)
            raise RuntimeError('Integration helper failed: '+action)
        data = json.loads(r.stdout)
        print(json.dumps({'action': action, 'scope': test_scope,
                          'sent': data.get('sent'), 'classified': data.get('classified')}), flush=True)
        return data
    counter = 0
    def consume(test_scope='production', fail=False):
        nonlocal counter
        counter += 1
        print('Consuming: '+test_scope+' attempt '+str(counter), flush=True)
        command = ['docker', 'compose', '--profile', 'e2', '--profile', 'streaming', 'run', '--rm', '--no-deps',
                   '--name', f'lifemap-stream-phase2-{scope}-{counter}', 'streaming',
                   '/opt/lifemap/streaming/consumer.py', '--scope', test_scope, '--available-now']
        if fail: command += ['--simulate-sink-failure']
        started = time.monotonic()
        r = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=300)
        log = r.stdout + r.stderr
        (output/f'consumer-{counter}.log').write_text(log, encoding='utf-8')
        if fail:
            assert r.returncode != 0 and 'Intentional test sink failure' in log, log[-2500:]
        elif r.returncode:
            print(log[-3500:], flush=True)
            raise RuntimeError('Structured Streaming failed')
        print(json.dumps({'consumer_exit': r.returncode, 'expected_failure': fail,
                          'seconds': round(time.monotonic()-started, 2)}), flush=True)
    real_published = helper('real')
    consume()
    real = helper('verify', expected=103)
    assert real['windows'], 'Real event times must close at least one window'
    first = [w for w in real['windows'] if w['start'].startswith('2019-01-01')]
    assert sum(w['reviews'] for w in first) == 100, 'Duplicate replays inflated first real window'
    helper('initial', scope)
    consume(scope, fail=True)
    partial = helper('verify', scope)
    assert 0 < partial['classified'] <= 30, 'Fault injection must happen after real Mongo writes'
    consume(scope)
    initial = helper('verify', scope, 30)
    assert initial['first_doc_processed_at'] == partial['first_doc_processed_at']
    helper('late', scope)
    consume(scope)
    within = helper('verify', scope, 31)
    helper('next', scope)
    consume(scope)
    before = helper('verify', scope, 60)
    assert len(before['windows']) == 1
    assert before['windows'][0]['reviews'] == 30 and before['windows'][0]['negative'] == 6
    helper('finish', scope)
    consume(scope)
    completed = helper('verify', scope, 61)
    assert len(completed['windows']) == 2 and len(completed['hotspots']) == 1
    assert sorted((w['reviews'], w['negative']) for w in completed['windows']) == [(30, 6), (30, 21)]
    assert abs(completed['hotspots'][0]['change_percentage_points']-50) < 1e-8
    helper('too_late', scope)
    consume(scope)
    after = helper('verify', scope, 62)
    assert after['windows'] == completed['windows'], 'Too-late message modified finalized windows'
    assert after['dropped_by_watermark'] >= 1
    assert after['first_doc_processed_at'] == initial['first_doc_processed_at']
    result = {'status': 'passed', 'scope': scope, 'real_published': real_published, 'real': compact_run(real),
              'fault_injection_partial_documents': partial['classified'],
              'sink_retry_idempotent': True, 'offset_checkpoint_resumed': True,
              'late_within_seven_days_included': True, 'too_late_excluded_from_windows': True,
              'late_reviews_still_classified': True, 'synthetic_fixture': compact_run(after),
              'synthetic_hotspots_not_real_world_findings': True,
              'Azure_accessed': False, 'model_retrained': False}
    path = ROOT/'docs/reports/e2-phase2.json'
    path.write_text(json.dumps(result, indent=2, ensure_ascii=False)+'\n', encoding='utf-8')
    print('PHASE2_INTEGRATION_PASSED '+str(path), flush=True)


if __name__ == '__main__':
    main()
