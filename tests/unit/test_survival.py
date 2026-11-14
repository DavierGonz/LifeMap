"""Casos conocidos: KM, censura, conjuntos en riesgo y entrada tardia."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from analytics.survival import kaplan_meier, prepare, validate_polygon, validate_real_curve


def observation(exit, event, entry=0):
    return {'entry': entry, 'exit': exit, 'event': event}


class SurvivalTests(unittest.TestCase):
    def test_known_censoring_fixture_reused_from_discos(self):
        rows = [observation(1,1), observation(2,0), observation(3,1)]
        result = kaplan_meier(rows, minimum=1)
        p = {r['time_days']:r for r in result['points']}
        self.assertAlmostEqual(p[1]['survival'], 2/3)
        self.assertAlmostEqual(p[2]['survival'], 2/3)
        self.assertEqual([p[t]['at_risk'] for t in (1,2,3)], [3,2,1])
        self.assertEqual(result['median_days'], 3)
        self.assertEqual(result['censored'], 1)
        self.assertEqual(p[3]['survival'], 0)
        self.assertGreater(validate_real_curve(rows, result), 0)

    def test_all_censored_does_not_mean_all_closed_or_infinite_lifetime(self):
        r = kaplan_meier([observation(1,0), observation(2,0)], minimum=1)
        self.assertEqual(r['events'], 0)
        self.assertEqual(r['censored'], 2)
        self.assertTrue(all(p['survival']==1 for p in r['points']))
        self.assertIsNone(r['median_days'])

    def test_tied_events_and_censors_share_pre_exit_risk_set(self):
        r = kaplan_meier([observation(2,1), observation(2,0), observation(3,0)], minimum=1)
        p = next(p for p in r['points'] if p['time_days']==2)
        self.assertEqual((p['at_risk'],p['events'],p['censored']), (3,1,1))
        self.assertAlmostEqual(p['survival'], 2/3)

    def test_identified_delayed_entry_changes_denominator(self):
        rows = [observation(2,1), observation(4,0), observation(5,1,entry=3)]
        r = kaplan_meier(rows, minimum=1)
        p = {p['time_days']:p for p in r['points']}
        self.assertEqual(p[2]['at_risk'], 2)
        self.assertEqual(p[2]['survival'], 0.5)
        self.assertEqual(p[4]['at_risk'], 2)
        self.assertEqual(r['delayed_entries'], 1)
        self.assertGreater(validate_real_curve(rows, r), 0)

    def test_positive_entry_at_event_time_joins_after_event(self):
        rows = [observation(2,1), observation(5,0), observation(4,1,entry=2)]
        r = kaplan_meier(rows, minimum=1)
        p = next(p for p in r['points'] if p['time_days']==2)
        self.assertEqual((p['at_risk'],p['entered'],p['remaining']), (2,1,2))
        self.assertEqual(p['survival'], 0.5)

    def test_zero_duration_and_small_group_are_not_silently_removed(self):
        rows = [observation(0,1), observation(0,0)]
        r = kaplan_meier(rows, minimum=1)
        self.assertEqual(r['points'][0]['survival'], 0.5)
        self.assertEqual(r['points'][0]['at_risk'], 2)
        self.assertEqual(r['median_days'], 0)
        r = kaplan_meier(rows, minimum=50)
        self.assertEqual(r['status'], 'insufficient_sample')
        self.assertEqual((r['events'],r['censored']), (1,1))
        self.assertEqual(r['points'], [])

    def test_invalid_intervals_fail(self):
        for r in [observation(1,1,2),observation(1,2),observation(float('nan'),1),observation(2,1,2)]:
            with self.assertRaises(ValueError): kaplan_meier([r],minimum=1)

    def test_real_definition_keeps_open_business_and_known_landmark_entry(self):
        first = datetime(2018,1,1,tzinfo=timezone.utc)
        base = dict(business_id='b',first_review=first,last_review=first+timedelta(days=10),
                    dataset_cutoff=first+timedelta(days=800),business_days=10,is_open=1,event=0,censored=True)
        rows, excluded = prepare([base], '2019-01-01')
        self.assertEqual((rows[0]['entry'],rows[0]['exit'],rows[0]['event']), (365,800,0))
        self.assertFalse(excluded)
        # Same review bounds but closed: event predates landmark, not at risk there.
        rows, excluded = prepare([{**base,'is_open':0,'event':1,'censored':False}], '2019-01-01')
        self.assertFalse(rows)
        self.assertEqual(excluded, {'ended_before_or_at_landmark':1})

    def test_polygon_is_closed_and_wgs84(self):
        import json
        config=json.loads((Path(__file__).resolve().parents[2]/'config/survival.json').read_text())
        for z in config['zones']: self.assertEqual(validate_polygon(z['geometry']), z['geometry'])
        for geometry in [{'type':'Point','coordinates':[0,0]},
                         {'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1]]]},
                         {'type':'Polygon','coordinates':[[[0,0],[181,0],[1,1],[0,0]]]}]:
            with self.assertRaises(ValueError): validate_polygon(geometry)


if __name__ == '__main__':
    unittest.main()
