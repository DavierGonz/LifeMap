"""Contrato temporal del productor, sin Kafka ni dependencias de host."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from streaming.producer import instant, publish, validate_model


class ProducerTests(unittest.TestCase):
    def row(self, id='r1', date='2020-01-02', business='b1'):
        return dict(review_id=id, date=date, business_id=business, text='Good food',
                    stars=5, sentiment=2, sentiment_model='old')

    def test_clock_key_payload_and_timestamp(self):
        now = [0.0]
        sent = []
        def send(key, value, timestamp):
            sent.append((key, value, timestamp))
            return SimpleNamespace(partition=1)
        result = publish([self.row(), self.row('r2', '2020-02-01')], send,
                         '2020-01-01', seconds_per_day=1/3,
                         clock=lambda: now[0], sleep=lambda d: now.__setitem__(0, now[0]+d),
                         model_id='temporal-v1', run_id='test')
        self.assertEqual(result['sent'], 2)
        self.assertAlmostEqual(now[0], 10)
        self.assertEqual(sent[0][0], b'b1')
        self.assertEqual(sent[0][2], int(instant('2020-01-02').timestamp()*1000))
        self.assertNotIn('stars', sent[0][1])
        self.assertNotIn('sentiment', sent[0][1])
        self.assertEqual(sent[1][1]['sequence'], 1)

    def test_rejects_training_dates_including_boundary(self):
        for date in ['2019-12-31', '2020-01-01']:
            with self.assertRaisesRegex(ValueError, 'strictly after'):
                publish([self.row(date=date)], lambda *a: self.fail('must not send'), '2020-01-01')

    def test_database_sort_delay_does_not_advance_replay_clock(self):
        now = [0.0]
        sleeps = []
        def rows():
            now[0] += 20  # MongoDB prepares its external chronological sort.
            yield self.row()
            yield self.row('r2', '2020-02-01')
        def sleep(delay):
            sleeps.append(delay)
            now[0] += delay
        publish(rows(), lambda *a: SimpleNamespace(partition=0), '2020-01-01', 1/3,
                clock=lambda: now[0], sleep=sleep)
        self.assertEqual(sleeps, [10.0])

    def test_rejects_out_of_order_and_duplicate_review(self):
        for rows in [[self.row('r2'), self.row('r1')], [self.row(), self.row()]]:
            with self.assertRaisesRegex(ValueError, 'strictly sorted'):
                publish(rows, lambda *a: SimpleNamespace(partition=0), '2020-01-01', 0)

    def test_limit_and_speed_validation(self):
        result = publish([self.row(), self.row('r2')], lambda *a: SimpleNamespace(partition=0),
                         '2020-01-01', 0, limit=1)
        self.assertEqual(result['sent'], 1)
        for speed in [-1, float('nan'), float('inf')]:
            with self.assertRaises(ValueError):
                publish([], None, '2020-01-01', speed)

    def test_model_requires_proof_cutoff_and_integrity(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'predictor.json').write_text('{}')
            manifest = dict(temporal_training_verified=True, training_max_date='2019-12-30',
                            selection_max_date='2019-12-31', predictor='predictor.json',
                            sha256=hashlib.sha256(b'{}').hexdigest(), model_id='v1',
                            training_review_ids_sha256='a'*64)
            self.assertEqual(validate_model(manifest, '2020-01-01', directory), 'v1')
            for updates in [{'temporal_training_verified': False},
                            {'training_max_date': '2020-01-02'},
                            {'selection_max_date': '2020-01-02'}, {'sha256': 'wrong'},
                            {'predictor': '../escape'}, {'training_review_ids_sha256': ''}]:
                with self.assertRaises((ValueError, FileNotFoundError)):
                    validate_model({**manifest, **updates}, '2020-01-01', directory)


if __name__ == '__main__':
    unittest.main()
