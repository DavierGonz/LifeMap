"""Contratos minimos de version, idempotencia y geometria, sin Spark en host."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.geo import geometry
from streaming.sinks import bind_checkpoint, collections, hotspot, save_windows


class StreamingTests(unittest.TestCase):
    def test_hotspot_requires_consecutive_sufficient_samples_and_percentage_points(self):
        self.assertEqual(hotspot({'reviews': 30, 'negative': 21}, {'reviews': 30, 'negative': 6}), (True, 49.99999999999999))
        self.assertFalse(hotspot({'reviews': 29, 'negative': 29}, {'reviews': 30, 'negative': 0})[0])
        self.assertEqual(hotspot({'reviews': 30, 'negative': 30}, None), (False, None))
        self.assertFalse(hotspot({'reviews': 100, 'negative': 20}, {'reviews': 100, 'negative': 10})[0])

    def test_checkpoint_cannot_change_model_or_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            binding = {'model': 'temporal-v1', 'sha': 'abc', 'watermark': '7 days'}
            bind_checkpoint(directory, binding)
            bind_checkpoint(directory, binding)
            for key in binding:
                with self.assertRaises(ValueError):
                    bind_checkpoint(directory, {**binding, key: 'changed'})

    def test_collections_never_target_e1(self):
        for scope in ['production', 'test_restart']:
            self.assertFalse(set(collections(scope)) & {'reviews', 'businesses', 'model_runs', 'closure_stats'})
        for scope in ['reviews', '../production', 'test_/bad']:
            with self.assertRaises(ValueError):
                collections(scope)

    def test_window_writer_handles_reverse_order_and_retry_without_incrementing(self):
        from types import SimpleNamespace as S
        class Collection:
            def __init__(self): self.docs = {}
            def update_one(self, key, update, upsert=False):
                if key['_id'] not in self.docs:
                    self.docs[key['_id']] = {**key, **update.get('$setOnInsert', {})}
                self.docs[key['_id']].update(update.get('$set', {}))
            def find_one(self, key): return self.docs.get(key['_id'])
            def replace_one(self, key, doc, upsert=False): self.docs[key['_id']] = doc
        class DB(dict):
            client = S(close=lambda: None)
            def __missing__(self, key): self[key] = Collection(); return self[key]
        db = DB();start = datetime(2019, 1, 1)
        rows = [S(geohash='dr4e3', window=S(start=start+timedelta(days=30), end=start+timedelta(days=60)), reviews=30, negative=21),
                S(geohash='dr4e3', window=S(start=start, end=start+timedelta(days=30)), reviews=30, negative=6)]
        frame = S(toLocalIterator=lambda: iter(rows))
        config = {'hotspot_min_reviews': 30, 'hotspot_increase_percentage_points': 15}
        with patch('streaming.sinks.writer', return_value=db):
            save_windows(frame, 0, 'production', {'model_id': 'v1', 'sha256': 'abc'}, config)
            save_windows(frame, 0, 'production', {'model_id': 'v1', 'sha256': 'abc'}, config)
        self.assertEqual(len(db['stream_windows'].docs), 2)
        self.assertEqual(len(db['hotspots'].docs), 1)
        cell = next(iter(db['hotspots'].docs.values()))['geometry']
        self.assertEqual(cell['type'], 'Polygon')
        self.assertEqual(cell['coordinates'][0][0], cell['coordinates'][0][-1])


if __name__ == '__main__':
    unittest.main()
