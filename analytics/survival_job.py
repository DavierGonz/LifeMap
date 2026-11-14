"""Calcula curvas descriptivas desde Mongo; sin endpoints, entrenamiento o Spark."""
from collections import defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import resource

from analytics.common import save
from analytics.survival import fetch_businesses, kaplan_meier, prepare, utc, validate_real_curve
from storage.yelp import database


def main():
    started = time.monotonic()
    config = json.loads(Path('/app/config/survival.json').read_text())
    db = database()
    try:
        rows = fetch_businesses(db)
        assert len(rows) == 16944
        assert all(r.get('entry_age_days') is None for r in rows), 'Review newly identifiable birth-age information before reuse'
        fingerprint = hashlib.sha256(json.dumps({'rows': rows, 'config': config}, sort_keys=True, default=str).encode()).hexdigest()
        directory = Path('/data/lifemap-e2/survival')/fingerprint[:16]
        directory.mkdir(parents=True, exist_ok=True)
        categories = defaultdict(list)
        for row in rows: categories[row['category_primary']].append(row)
        groups = [('category', k, v, None) for k,v in sorted(categories.items())]
        zone_ids = []
        for zone in config['zones']:
            selected = fetch_businesses(db, polygon=zone['geometry'])
            assert len(selected) >= config['minimum_businesses']
            zone_ids.append({r['business_id'] for r in selected})
            groups.append(('zone', zone['id'], selected, zone['geometry']))
        assert not zone_ids[0] & zone_ids[1], 'Comparison rectangles must not overlap'
        samples = []
        for category in config['comparison_categories']:
            assert len(categories[category]) >= config['minimum_businesses']
            samples += categories[category][:2]
        # Reuse stored life; verify only four indexed review timelines, no 1.3M reprocessing.
        for b in samples:
            a = db.reviews.find_one({'business_id': b['business_id']}, sort=[('date', 1)])
            z = db.reviews.find_one({'business_id': b['business_id']}, sort=[('date', -1)])
            assert utc(a['date']) == utc(b['first_review']) and utc(z['date']) == utc(b['last_review'])
        docs, direct_checks = [], 0
        for kind, id, subset, polygon in groups:
            for method, landmark in [('observed_origin', None), ('landmark_T', config['landmark'])]:
                records, excluded = prepare(subset, landmark)
                result = kaplan_meier(records, config['minimum_businesses'], config['minimum_at_risk_for_interpretation'])
                direct_checks += validate_real_curve(records, result)
                docs.append(dict(_id=f'{fingerprint}:{method}:{kind}:{id}', run_id=fingerprint,
                    group_type=kind, group_id=id, method=method, polygon=polygon, **result,
                    source_businesses=len(subset), exclusions=excluded, descriptive_retrospective=True,
                    allowed_as_risk_predictor=False, uses_post_T=True,
                    origin='first_review; not actual opening', event_proxy='last_review when is_open=0',
                    censoring='dataset_cutoff when is_open=1 (Word administrative snapshot)',
                    true_birth_age_entry_identifiable=False,
                    entry='0 on observed clock' if landmark is None else 'max(0,T-first_review); observed-age left truncation only',
                    landmark=landmark))
        db.survival_results.create_index([('run_id', 1), ('group_type', 1), ('group_id', 1), ('method', 1)], unique=True, name='run_group_method')
        for doc in docs: db.survival_results.replace_one({'_id': doc['_id']}, doc, upsert=True)
        assert db.survival_results.count_documents({'run_id': fingerprint}) == len(docs)
        save(directory/'curves.json', {'run_id': fingerprint, 'config': config, 'curves': docs})
        group_fields = ['run_id','method','group_type','group_id','businesses','events','censored','delayed_entries','status','median_days','median_at_risk','median_supported']
        with (directory/'groups.csv').open('w', newline='', encoding='utf-8') as f:
            out = csv.DictWriter(f, fieldnames=group_fields);out.writeheader()
            for d in docs: out.writerow({k:d[k] for k in group_fields})
        point_fields = ['run_id','method','group_type','group_id','time_days','at_risk','events','censored','entered','remaining','survival','supported']
        with (directory/'points.csv').open('w', newline='', encoding='utf-8') as f:
            out = csv.DictWriter(f, fieldnames=point_fields);out.writeheader()
            for d in docs:
                for point in d['points']: out.writerow({**{k:d[k] for k in point_fields[:4]}, **point})
        comparisons = [{k:v for k,v in d.items() if k not in ['points','_id','run_id']} for d in docs
                       if d['group_type']=='zone' or d['group_id'] in config['comparison_categories']]
        summary = dict(run_id=fingerprint, source_businesses=len(rows), open=sum(r['is_open']==1 for r in rows),
                       closed=sum(r['is_open']==0 for r in rows), unknown_birth_entry_age=len(rows),
                       dataset_cutoff=str(rows[0]['dataset_cutoff']), category_groups=len(categories),
                       eligible_categories=sum(len(v)>=config['minimum_businesses'] for v in categories.values()),
                       saved_documents=len(docs), direct_risk_set_checks=direct_checks, review_timelines_verified=len(samples),
                       comparisons=comparisons, directory=str(directory), seconds=round(time.monotonic()-started,3),
                       peak_rss_mib=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,2),
                       true_opening_age_recovered=False, temporal_risk_features_created=False)
        save(directory/'summary.json', summary)
        print(json.dumps(summary), flush=True)
    finally:
        db.client.close()


if __name__ == '__main__':
    main()
