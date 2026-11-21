"""Casos pequeños de causalidad temporal, densidad, decisiones y probabilidades."""
import math
from pathlib import Path
import sys
from types import SimpleNamespace as S
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from ml.risk import FEATURES, competitor_density, confusion, feature_values, probability, select_threshold, trend


class RiskTests(unittest.TestCase):
    def baseline(self):
        return dict(n=2,sentiment_sum=0,stars_sum=6,first='2018-01-01',recent=[
            {'date':'2018-07-01','negative':1},{'date':'2018-12-01','negative':0}])

    def test_future_events_and_repeated_ids_cannot_change_features(self):
        event=dict(review_id='r',date='2019-01-02',stars=4,prediction=2)
        future=dict(review_id='future',date='2020-01-01',stars=1,prediction=0)
        values,q=feature_values(self.baseline(),[event,future,event],'2019-01-02',0)
        self.assertEqual(values['review_count'],3)
        self.assertAlmostEqual(values['mean_sentiment'],1/3)
        self.assertAlmostEqual(values['mean_stars'],10/3)
        self.assertEqual(values['observable_age_days'],366)
        self.assertEqual(set(values),set(FEATURES))
        self.assertNotIn('is_open',values)

    def test_baseline_after_T_and_scoring_before_T_are_rejected(self):
        b=self.baseline();b['recent'].append({'date':'2019-02-01','negative':1})
        with self.assertRaises(ValueError):feature_values(b,[],'2019-03-01',0)
        with self.assertRaises(ValueError):feature_values(self.baseline(),[],'2018-12-31',0)

    def test_six_month_trend_ignores_future_and_handles_missing_months(self):
        recent=self.baseline()['recent']+[{'date':'2020-01-01','negative':1}]
        slope,bins=trend(recent,'2019-01-01')
        self.assertAlmostEqual(slope,-20)
        self.assertEqual(bins,2)
        self.assertEqual(trend([{'date':'2018-12-01','negative':1}],'2019-01-01'),(0,1))

    def test_density_is_geoNear_known_presence_not_future_closed_status(self):
        captured={}
        class DB:
            risk_catalog=S(aggregate=lambda p:(captured.update(pipeline=p) or [{'n':2}]))
            def __getitem__(self,key):
                return S(distinct=lambda field,query:(captured.update(known_query=query) or ['seen']))
        business={'_id':'self','category_primary':'Pizza','location':{'type':'Point','coordinates':[-75.1,40]}}
        result=competitor_density(DB(),business,'2019-02-01')
        self.assertAlmostEqual(result,2/(math.pi*.25))
        stage=captured['pipeline'][0]['$geoNear']
        self.assertEqual(stage['maxDistance'],500)
        self.assertEqual(stage['query']['_id'],{'$ne':'self'})
        self.assertNotIn('is_open',stage['query'])
        self.assertEqual(stage['query']['$or'][1],{'_id':{'$in':['seen']}})

    def test_saved_logistic_score_and_confusion(self):
        model=dict(features=FEATURES,means=[0]*6,std=[1]*6,coefficients=[0]*6,intercept=0)
        self.assertEqual(probability(dict.fromkeys(FEATURES,1),model),.5)
        rows=[{'label':1,'probability':.8},{'label':0,'probability':.7},{'label':0,'probability':.1}]
        result=confusion(rows,.6)
        self.assertEqual((result['tp'],result['fp'],result['tn'],result['fn']),(1,1,1,0))
        self.assertEqual(result['recall'],1)

    def test_threshold_rule_uses_only_supplied_validation_rows(self):
        rows=[{'label':1,'probability':.8}]+[{'label':0,'probability':x} for x in [.6,.5,.4,.3,.2,.1]]
        selected=select_threshold(rows)
        self.assertTrue(selected['precision_constraint_satisfied'])
        actual=confusion(rows,selected['threshold'])
        self.assertEqual(selected['f2'],actual['f2'])
        self.assertGreaterEqual(actual['precision'],2/len(rows))


if __name__=='__main__':unittest.main()
