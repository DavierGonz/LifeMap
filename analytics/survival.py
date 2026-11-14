"""Kaplan-Meier sobre edad observada; admite truncamiento izquierdo identificable."""
from collections import Counter
from datetime import datetime, timezone
import math

DAY = 86400


def utc(value):
    if isinstance(value, str): value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None: value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def prepare(rows, landmark=None):
    """No infiere fundacion. La entrada positiva solo mide edad desde primera reseña."""
    records = []
    excluded = Counter()
    landmark = utc(landmark) if landmark else None
    for r in rows:
        first, last, cutoff = (utc(r[k]) for k in ('first_review', 'last_review', 'dataset_cutoff'))
        if not first <= last <= cutoff or r['is_open'] not in (0, 1):
            raise ValueError('Invalid life observation')
        event = int(r['is_open'] == 0)
        if r['event'] != event or bool(r['censored']) != (not event):
            raise ValueError('Stored E1 event/censoring flags disagree with is_open')
        span = (last-first).total_seconds()/DAY
        if not math.isclose(r['business_days'], span, abs_tol=1e-8):
            raise ValueError('Stored E1 observed duration disagrees with review bounds')
        # Word: is_open=1 means alive at dataset snapshot; this is administrative
        # censoring, not an invented opening/closure date or a new review date.
        end = last if event else cutoff
        if landmark and end <= landmark:
            excluded['ended_before_or_at_landmark'] += 1
            continue
        entry = max(0.0, (landmark-first).total_seconds()/DAY) if landmark else 0.0
        records.append(dict(business_id=r['business_id'], entry=entry,
                            exit=(end-first).total_seconds()/DAY, event=event,
                            observed_span_days=span))
    return records, dict(excluded)


def kaplan_meier(records, minimum=50, minimum_at_risk=20):
    """S(t)=product(1-d/n). Positive delayed entries use (entry, exit].

    Entry-zero baseline subjects are present at t=0, including zero-duration
    observations. At ties: events precede censoring; positive entries at that
    instant join the risk set after these exits (counting-process convention).
    """
    if minimum < 1 or minimum_at_risk < 1: raise ValueError('Positive sample thresholds required')
    entries, deaths, censors = Counter(), Counter(), Counter()
    for r in records:
        a, b, event = r['entry'], r['exit'], r['event']
        if not all(math.isfinite(x) for x in (a, b)) or a < 0 or b < a or (a > 0 and b == a) or event not in (0, 1):
            raise ValueError('Invalid (entry, exit, event) interval')
        entries[a] += 1
        (deaths if event else censors)[b] += 1
    result = dict(businesses=len(records), events=sum(deaths.values()), censored=sum(censors.values()),
                  delayed_entries=sum(n for t,n in entries.items() if t > 0), minimum_businesses=minimum,
                  minimum_at_risk_for_interpretation=minimum_at_risk,
                  median_days=None, median_at_risk=None, median_supported=False,
                  median_reason='not_reached', points=[])
    if len(records) < minimum:
        result.update(status='insufficient_sample', median_reason='insufficient_sample')
        return result
    survival, alive = 1.0, 0
    times = sorted(set(entries) | set(deaths) | set(censors))
    for t in times:
        entered, d, c = entries[t], deaths[t], censors[t]
        at_risk = alive + (entered if t == 0 else 0)
        if d+c > at_risk: raise ValueError('Exits exceed the risk set')
        if d:
            survival *= 1-d/at_risk
        alive += entered-d-c
        point = dict(time_days=t, at_risk=at_risk, events=d, censored=c,
                     entered=entered, remaining=alive, survival=survival,
                     supported=at_risk >= minimum_at_risk)
        result['points'].append(point)
        if survival <= 0.5 and result['median_days'] is None:
            result.update(median_days=t, median_at_risk=at_risk,
                          median_supported=at_risk >= minimum_at_risk, median_reason='reached')
    assert alive == 0
    result.update(status='ok', support_start_days=min(entries), support_end_days=max(times))
    return result


def validate_polygon(polygon):
    """Poligonos simples de estudio: anillo unico, cerrado, coordenadas WGS84."""
    if polygon.get('type') != 'Polygon' or len(polygon.get('coordinates', [])) != 1:
        raise ValueError('Expected a single-ring GeoJSON Polygon')
    ring = polygon['coordinates'][0]
    if len(ring) < 4 or ring[0] != ring[-1]: raise ValueError('Polygon ring must be closed')
    for point in ring:
        if len(point) != 2 or not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) for v in point):
            raise ValueError('Finite longitude/latitude required')
        if not -180 <= point[0] <= 180 or not -90 <= point[1] <= 90:
            raise ValueError('Coordinates outside WGS84 bounds')
    area = sum(a[0]*b[1]-b[0]*a[1] for a,b in zip(ring, ring[1:]))
    if abs(area) < 1e-12: raise ValueError('Polygon has zero area')
    # MongoDB also validates spherical geometry; configured zones are simple rectangles.
    return polygon


FIELDS = {'_id': 0, **{k: 1 for k in ('business_id', 'category_primary', 'first_review', 'last_review',
          'dataset_cutoff', 'business_days', 'is_open', 'event', 'censored', 'entry_age_days')}}


def fetch_businesses(db, category=None, polygon=None):
    """Reusable por el futuro endpoint; la seleccion espacial ocurre en MongoDB."""
    query = {}
    if category is not None: query['category_primary'] = category
    if polygon is not None: query['location'] = {'$geoWithin': {'$geometry': validate_polygon(polygon)}}
    return list(db.businesses.find(query, FIELDS).sort('_id', 1).batch_size(500))


def validate_real_curve(records, result):
    """Contraste directo de risk sets en puntos reales, sin repetir todo O(n^2)."""
    if result['status'] != 'ok': return 0
    points = result['points']
    sample = points[::max(1, len(points)//10)] + points[-1:]
    for p in sample:
        t = p['time_days']
        expected = sum((r['entry'] < t or r['entry'] == t == 0) and r['exit'] >= t for r in records)
        assert p['at_risk'] == expected
    assert sum(p['events'] for p in points) == result['events']
    assert sum(p['censored'] for p in points) == result['censored']
    assert all(a['survival'] >= b['survival'] >= 0 for a,b in zip(points, points[1:]))
    return len(sample)
