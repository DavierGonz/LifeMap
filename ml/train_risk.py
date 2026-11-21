"""Entrenamiento retrospectivo MLlib. Solo reutiliza el modelo de sentimiento T."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import time
from analytics.common import save, session
from ml.risk import FEATURES, feature_values, competitor_density, probability, select_threshold, confusion
from storage.yelp import database
from streaming.producer import instant, validate_model


def main():
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.sql import functions as F
    from pymongo import ReplaceOne
    config=json.loads(Path('/opt/lifemap/config/risk.json').read_text())
    model_id=config['model_id'];cutoff=instant(config['cutoff']);horizon=instant(config['horizon_end'])
    directory=Path('/data/lifemap-e2')/model_id;directory.mkdir(parents=True,exist_ok=True)
    root=Path('/models/risk')/model_id
    if (directory/'training.json').exists():
        print('RISK_CACHE_HIT', (directory/'training.json').read_text(),flush=True);return
    if root.exists(): raise ValueError('Partial risk artifact exists; inspect before retry; no automatic overwrite')
    sentiment=json.loads(Path('/models/sentiment-temporal.json').read_text())
    assert validate_model(sentiment,cutoff,'/models')==config['sentiment_model_id'] and sentiment['sha256']==config['sentiment_sha256']
    from streaming.sinks import pipeline_hash
    stream_config=json.loads(Path('/opt/lifemap/config/streaming.json').read_text())
    assert pipeline_hash(Path('/models')/sentiment['spark_pipeline'])==stream_config['spark_pipeline_sha256']
    db=database();spark=session('risk-training');started=time.monotonic()
    try:
        source=Path('/data/lifemap-e2/sentiment-until-20190101T000000Z-v1')
        summary_path=directory/'baseline-summary'
        if not (summary_path/'_SUCCESS').exists():
            saved=PipelineModel.load('/models/'+sentiment['spark_pipeline'])
            frame=saved.stages[-1].transform(spark.read.parquet(str(source/'features')))
            dates=spark.read.parquet(str(source/'base')).select('review_id','date')
            frame=frame.select('review_id','business_id','prediction').join(dates,'review_id')
            from ml.risk import shift_months
            grouped=frame.groupBy('business_id').agg(F.count('*').alias('n'),F.sum(F.col('prediction')-1).alias('sentiment_sum'),
                F.collect_list(F.when(F.col('date')>=F.lit(shift_months(cutoff,-6).replace(tzinfo=None)),
                   F.struct('date',(F.col('prediction')==0).cast('int').alias('negative')))).alias('recent'))
            grouped.write.mode('errorifexists').parquet(str(summary_path))
            print('RISK_BASELINE_SENTIMENT_READY',flush=True)
        stats={r['_id']:r for r in db.reviews.aggregate([{'$match':{'date':{'$lte':cutoff}}},
               {'$group':{'_id':'$business_id','n':{'$sum':1},'stars_sum':{'$sum':'$stars'},'first':{'$min':'$date'}}}],allowDiskUse=True)}
        assert sum(s['n'] for s in stats.values())==994877
        businesses={r['business_id']:r for r in db.businesses.find({}, {'business_id':1,'first_review':1,'last_review':1,'is_open':1,'category_primary':1,'location':1})}
        catalog=[{'_id':id,'first_review':b['first_review'],'category_primary':b['category_primary'],'location':b['location']} for id,b in sorted(businesses.items())]
        catalog_by_id={r['_id']:r for r in catalog}
        catalog_sha=hashlib.sha256(json.dumps(catalog,sort_keys=True,default=str).encode()).hexdigest()
        previous=db.risk_metadata.find_one({'_id':'catalog'})
        if previous and previous['sha256']!=catalog_sha: raise ValueError('Frozen spatial catalog differs')
        db.risk_catalog.bulk_write([ReplaceOne({'_id':r['_id']},r,upsert=True) for r in catalog],ordered=False)
        db.risk_catalog.create_index([('location','2dsphere')],name='location_2dsphere')
        db.risk_metadata.replace_one({'_id':'catalog'},{'_id':'catalog','sha256':catalog_sha,'static_location_category_assumption':True},upsert=True)
        eligible=[];writes=[];baseline_count=0
        for row in spark.read.parquet(str(summary_path)).toLocalIterator():
            id=row.business_id;s=stats[id];assert row.n==s['n']
            baseline={'_id':id,'n':row.n,'sentiment_sum':float(row.sentiment_sum),'stars_sum':float(s['stars_sum']),
                      'first':s['first'],'recent':[r.asDict() for r in row.recent],
                      'as_of':cutoff,'sentiment_model_id':sentiment['model_id']}
            writes.append(ReplaceOne({'_id':id},baseline,upsert=True));baseline_count+=1
            if len(writes)==500: db.risk_baselines.bulk_write(writes,ordered=False);writes=[]
            b=businesses[id];last=instant(b['last_review'])
            if last<=cutoff: continue
            label=int(b['is_open']==0 and last<=horizon)
            assert label==1 or b['is_open']==1 or last>horizon
            density=competitor_density(db,catalog_by_id[id],cutoff)
            values,quality=feature_values(baseline,[],cutoff,density)
            eligible.append({'business_id':id,'label':float(label),**values})
        if writes: db.risk_baselines.bulk_write(writes,ordered=False)
        assert len(eligible)==10974 and sum(r['label'] for r in eligible)==709
        for label in [0,1]:
            cohort=sorted([r for r in eligible if r['label']==label],key=lambda r:hashlib.sha256((config['split_seed']+r['business_id']).encode()).hexdigest())
            for i,r in enumerate(cohort):r['split']='train' if i<int(len(cohort)*.7) else 'validation' if i<int(len(cohort)*.85) else 'test'
        save(directory/'features.json',eligible)
        distribution={s:dict(Counter(int(r['label']) for r in eligible if r['split']==s)) for s in ['train','validation','test']}
        train_counts=distribution['train'];weights={k:sum(train_counts.values())/(2*v) for k,v in train_counts.items()}
        for r in eligible:r['weight']=weights[int(r['label'])]
        frame=spark.createDataFrame(eligible)
        estimator=Pipeline(stages=[VectorAssembler(inputCols=FEATURES,outputCol='raw_features'),
                  StandardScaler(inputCol='raw_features',outputCol='features',withMean=True,withStd=True),
                  LogisticRegression(featuresCol='features',labelCol='label',weightCol='weight',maxIter=config['max_iterations'],regParam=config['regularization'])])
        model=estimator.fit(frame.filter("split='train'"))
        def predictions(split):
            scored=model.transform(frame.filter(F.col('split')==split))
            rows=[{'business_id':r.business_id,'label':r.label,'probability':float(r.probability[1])} for r in scored.select('business_id','label','probability').collect()]
            metrics={name:BinaryClassificationEvaluator(labelCol='label',metricName=name,numBins=0).evaluate(scored) for name in ['areaUnderROC','areaUnderPR']}
            return rows,metrics
        validation,val_metrics=predictions('validation');selection=select_threshold(validation)
        threshold=selection['threshold'];test,test_metrics=predictions('test')
        scaler,lr=model.stages[1:]
        exported={'model_id':model_id,'features':FEATURES,'feature_version':config['feature_version'],'cutoff':config['cutoff'],
                  'means':list(scaler.mean),'std':list(scaler.std),'coefficients':list(lr.coefficients),
                  'intercept':float(lr.intercept),'threshold':threshold,'catalog_sha256':catalog_sha,
                  'sentiment_model_id':sentiment['model_id'],'retrospective_simulation_only':True,
                  'probability_is_uncalibrated_class_weighted_score':True}
        lookup={r['business_id']:r for r in eligible}
        for r in test[:30]:assert abs(probability(lookup[r['business_id']],exported)-r['probability'])<1e-8
        root.mkdir(parents=True);model.save(str(root/'spark_pipeline'));save(root/'predictor.json',exported)
        manifest={'model_id':model_id,'predictor':str((root/'predictor.json').relative_to('/models')),
                  'spark_pipeline':str((root/'spark_pipeline').relative_to('/models')),
                  'sha256':hashlib.sha256((root/'predictor.json').read_bytes()).hexdigest()}
        manifest['spark_pipeline_sha256']=pipeline_hash(root/'spark_pipeline')
        save('/models/risk-active.json',manifest)
        save(directory/'test-predictions.json',test)
        report={'model_id':model_id,'eligible':len(eligible),'labels':dict(Counter(int(r['label']) for r in eligible)),
                'split':distribution,'baseline_businesses':baseline_count,'historical_reviews':994877,
                'validation':{**val_metrics,**confusion(validation,threshold)},'test':{**test_metrics,**confusion(test,threshold)},
                'threshold_selection':selection,'threshold':threshold,'manifest':manifest,'features':FEATURES,
                'classifier_parameters':{'algorithm':'weighted LogisticRegression','maxIter':config['max_iterations'],'regParam':config['regularization'],'class_weights_train_only':weights},
                'scaler_fit_train_only':True,'threshold_chosen_validation_only':True,'business_overlap':0,
                'predictor_parity_samples':30,'catalog_sha256':catalog_sha,'static_metadata_stability_assumption':True,
                'retrospective_model_not_available_historically_at_T':True,'seconds':round(time.monotonic()-started,3),
                'artifact_directory':str(directory)}
        save(directory/'training.json',report);print('RISK_TRAINING_COMPLETE',json.dumps(report),flush=True)
    finally:spark.stop();db.client.close()


if __name__=='__main__':main()
