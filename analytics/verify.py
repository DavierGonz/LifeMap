"""Gate pequeno de resultados reales ya ejecutados; no repite entrenamientos."""
import json
from pathlib import Path
from ml.inference import load_active
from storage.yelp import database

def main():
    sentiment=json.loads(Path('/data/lifemap-e1/sentiment-active.json').read_text())
    analytics=json.loads(Path('/data/lifemap-e1/analytics-active.json').read_text())
    assert sentiment['business_overlap']==0 and sentiment['predictor_spark_parity_samples']>=30
    assert set(sentiment['models'])=={'naive_bayes','logistic_regression'}
    assert analytics['rdd_dataframe_equal'] and len(analytics['benchmark_times'])==24
    for n in [2,8,32,200]:
        for mode in ['city','round_robin']:
            assert len([row for row in analytics['benchmark_times'] if row['partitions']==n and row['mode']==mode])==3
    assert load_active()['model_id']==sentiment['model_id']
    db=database()
    try:
        assert db.businesses.count_documents({},hint='_id_')==16944
        assert db.reviews.count_documents({},hint='_id_')==1320594
        assert any(index['key']==[('location','2dsphere')] for index in db.businesses.index_information().values())
        assert db.analytics_state.find_one({'_id':'active'})['run_id']==analytics['run_id']
    finally: db.client.close()
    print(json.dumps({'passed':True,'businesses':16944,'reviews':1320594,'model_id':sentiment['model_id'],'benchmark_runs':24}))
if __name__=='__main__': main()
