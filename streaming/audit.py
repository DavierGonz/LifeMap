"""Auditoria de solo lectura del corpus y del entrenamiento E1; ejecutar en Dask."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

from streaming.producer import instant, reader


def audit(output):
    import pyarrow.parquet as pq
    client = reader()
    try:
        db = client.lifemap
        source = db.ingestion_runs.find_one({'kind': 'yelp_load', 'status': 'complete'})
        if not source:
            raise ValueError('Missing complete E1 ingestion')
        run = db.model_runs.find_one({'source_load': source['_id']})
        if not run:
            raise ValueError('Missing E1 training provenance')
        training_businesses = set()
        directory = Path(run['model_directory'])
        base = Path(run['prediction_path']).parent / 'text-base'
        for part in base.glob('*.parquet'):
            for batch in pq.ParquetFile(part).iter_batches(batch_size=4096, columns=['business_id', 'split']):
                training_businesses.update(r['business_id'] for r in batch.to_pylist() if r['split'] == 'train')
        if len(training_businesses) != run['business_split']['train']:
            raise ValueError('Training business manifest does not match E1 report')
        counts = Counter()
        months = Counter()
        first = last = training_max = None
        life = {}
        candidates = {f'{y}-01-01T00:00:00+00:00': Counter() for y in (2018, 2019, 2020, 2021)}
        bounds = {k: (instant(k), instant(f'{instant(k).year + 1}-01-01')) for k in candidates}
        for row in db.reviews.find({}, {'business_id': 1, 'date': 1, '_id': 0}).batch_size(2000):
            date = instant(row['date'])
            business = row['business_id']
            train = business in training_businesses
            counts['reviews'] += 1
            counts['training_reviews' if train else 'test_reviews'] += 1
            months[date.strftime('%Y-%m')] += 1
            first = min(first, date) if first else date
            last = max(last, date) if last else date
            if train:
                training_max = max(training_max, date) if training_max else date
            span = life.setdefault(business, [date, date])
            span[0], span[1] = min(span[0], date), max(span[1], date)
            for key, (cutoff, horizon) in bounds.items():
                c = candidates[key]
                c['before_or_at_T' if date <= cutoff else 'after_T'] += 1
                if date > horizon:
                    c['after_T_plus_12_months'] += 1
                if date > cutoff and train:
                    c['post_T_used_in_E1_training'] += 1
        businesses = list(db.businesses.find({}, {'business_id': 1, 'is_open': 1, '_id': 0}))
        for key, (cutoff, horizon) in bounds.items():
            c = candidates[key]
            for b in businesses:
                span = life.get(b['business_id'])
                if not span or span[0] > cutoff or span[1] <= cutoff:
                    continue
                c['businesses_with_history_and_post_T_activity'] += 1
                if b['is_open'] == 0 and span[1] <= horizon:
                    c['risk_proxy_positive'] += 1
                elif b['is_open'] == 1 or span[1] > horizon:
                    c['risk_proxy_negative'] += 1
            c['horizon_observed'] = last >= horizon
        pointer = json.loads(Path('/models/sentiment-active.json').read_text())
        report = {'counts': {'businesses': len(businesses), **counts},
                  'date_min': first.isoformat(), 'date_max': last.isoformat(),
                  'training_max_date': training_max.isoformat(),
                  'E1_model': pointer, 'training_businesses': len(training_businesses),
                  'monthly_reviews': dict(sorted(months.items())),
                  'candidates': candidates,
                  'E1_temporally_compatible': False,
                  'risk_label_is_retrospective_proxy': True,
                  'observed_dates_are_reviews_not_opening_or_closure': True}
        assert counts['reviews'] == 1320594 and len(businesses) == 16944
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(json.dumps({k: v for k, v in report.items() if k != 'monthly_reviews'}))
        return report
    finally:
        client.close()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', default='/data/lifemap-e2/temporal-audit.json')
    audit(p.parse_args().output)
