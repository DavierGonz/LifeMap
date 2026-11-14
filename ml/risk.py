"""Seis predictores segun tiempo disponible del evento; sin etiquetas ni curvas KM."""
import bisect
import calendar
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

from streaming.producer import instant

FEATURES = ['mean_sentiment', 'negative_trend_6m', 'mean_stars', 'review_count', 'observable_age_days', 'competitor_density_km2']


def shift_months(date, months):
    year, month = divmod(date.year*12 + date.month-1 + months, 12)
    return date.replace(year=year, month=month+1, day=min(date.day, calendar.monthrange(year, month+1)[1]))


def trend(recent, asof):
    asof = instant(asof)
    bounds = [shift_months(asof, i-6) for i in range(7)]
    total, negative = [0]*6, [0]*6
    for r in recent:
        date = instant(r['date'])
        if bounds[0] <= date <= asof:
            i = min(5, bisect.bisect_right(bounds, date)-1)
            total[i] += 1;negative[i] += int(r['negative'])
    points = [(i,100*negative[i]/n) for i,n in enumerate(total) if n]
    if len(points)<2: return 0.0, len(points)
    mx = sum(x for x,y in points)/len(points);my = sum(y for x,y in points)/len(points)
    return sum((x-mx)*(y-my) for x,y in points)/sum((x-mx)**2 for x,y in points), len(points)


def feature_values(baseline, events, asof, density, cutoff='2019-01-01'):
    asof, cutoff = instant(asof), instant(cutoff)
    if asof < cutoff: raise ValueError('Risk model cannot score before its feature cutoff')
    n = baseline['n'] if baseline else 0
    sentiment = baseline['sentiment_sum'] if baseline else 0.0
    stars = baseline['stars_sum'] if baseline else 0.0
    first = instant(baseline['first']) if baseline else None
    recent = list(baseline['recent']) if baseline else []
    if baseline and (first>cutoff or any(instant(r['date'])>cutoff for r in recent)):
        raise ValueError('Baseline contains information after cutoff')
    seen = set()
    for r in events:
        date = instant(r['date'])
        if not cutoff < date <= asof or r['review_id'] in seen: continue
        seen.add(r['review_id'])
        if not 1 <= r['stars'] <= 5: raise ValueError('Historical review stars are required')
        n += 1;sentiment += r['prediction']-1;stars += r['stars']
        first = min(first,date) if first else date
        recent.append({'date':date,'negative':int(r['prediction']==0)})
    if not n: raise ValueError('No observations available at the simulated instant')
    slope, bins = trend(recent, asof)
    values = [sentiment/n, slope, stars/n, float(n), (asof-first).total_seconds()/86400, float(density)]
    if not all(math.isfinite(x) for x in values): raise ValueError('Nonfinite risk feature')
    return dict(zip(FEATURES,values)), {'trend_observed_month_bins':bins,'cold_start':baseline is None}


def competitor_density(db, business, asof, scope='production', cutoff='2019-01-01'):
    """Known presence, not current open status; static geography/category assumption."""
    from streaming.sinks import collections
    asof, cutoff = instant(asof), instant(cutoff)
    known = []
    if asof > cutoff:
        known = db[collections(scope)[0]].distinct('business_id', {'date':{'$gt':cutoff,'$lte':asof}})
    query = {'category_primary':business['category_primary'], '_id':{'$ne':business['_id']},
             '$or':[{'first_review':{'$lte':cutoff}}, {'_id':{'$in':known}}]}
    result = list(db.risk_catalog.aggregate([{'$geoNear':{'near':business['location'],'key':'location',
                  'distanceField':'distance_m','spherical':True,'maxDistance':500,'query':query}}, {'$count':'n'}]))
    count = result[0]['n'] if result else 0
    return count/(math.pi*0.5**2)


def probability(values, model):
    vector = [values[k] for k in model['features']]
    scaled = [(x-m)/s if s else 0.0 for x,m,s in zip(vector,model['means'],model['std'])]
    score = model['intercept']+sum(a*b for a,b in zip(scaled,model['coefficients']))
    return 1/(1+math.exp(-score)) if score>=0 else math.exp(score)/(1+math.exp(score))


def confusion(rows, threshold):
    tp=fp=tn=fn=0
    for r in rows:
        actual=int(r['label']);positive=r['probability']>=threshold
        tp+=int(actual==1 and positive);fp+=int(actual==0 and positive)
        fn+=int(actual==1 and not positive);tn+=int(actual==0 and not positive)
    precision=tp/(tp+fp) if tp+fp else 0.0;recall=tp/(tp+fn) if tp+fn else 0.0
    return dict(tp=tp,fp=fp,tn=tn,fn=fn,precision=precision,recall=recall,
                f2=5*tp/(5*tp+4*fn+fp) if tp+fn+fp else 0.0)


def select_threshold(rows):
    prevalence=sum(int(r['label']) for r in rows)/len(rows)
    groups={}
    for r in rows:
        g=groups.setdefault(r['probability'],[0,0]);g[int(r['label'])]+=1
    scores=sorted(groups,reverse=True);tp=fp=0;positives=sum(int(r['label']) for r in rows)
    candidates=[]
    for i,score in enumerate(scores):
        negative,positive=groups[score];tp+=positive;fp+=negative
        precision=tp/(tp+fp);recall=tp/positives if positives else 0
        f2=5*tp/(5*tp+4*(positives-tp)+fp) if tp+fp+positives else 0
        lower=scores[i+1] if i+1<len(scores) else 0.0
        candidates.append(dict(threshold=(score+lower)/2,precision=precision,recall=recall,f2=f2))
    feasible=[c for c in candidates if c['precision']>=2*prevalence]
    best=max(feasible or candidates,key=lambda c:(c['f2'],c['recall'],c['precision']))
    return {**best,'validation_prevalence':prevalence,'precision_constraint_satisfied':bool(feasible)}


def load_risk(root='/models', config_path='/opt/lifemap/config/risk.json'):
    config=json.loads(Path(config_path).read_text());root=Path(root)
    manifest=json.loads((root/'risk-active.json').read_text())
    path=(root/manifest['predictor']).resolve()
    if not path.is_relative_to(root.resolve()): raise ValueError('Risk artifact outside model volume')
    payload=path.read_bytes()
    if hashlib.sha256(payload).hexdigest()!=config['model_sha256'] or manifest['sha256']!=config['model_sha256']:
        raise ValueError('Risk model differs from pinned version')
    model=json.loads(payload)
    if model['model_id']!=config['model_id'] or model['features']!=FEATURES: raise ValueError('Unexpected risk model contract')
    if manifest['spark_pipeline_sha256']!=config['spark_pipeline_sha256'] or model['cutoff']!=config['cutoff']:
        raise ValueError('Risk model temporal or native pipeline version differs')
    return model, manifest
