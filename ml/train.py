"""Entrena NB/LR por negocio, evalua y publica el modelo real de E1."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import quote

from analytics.common import save,session
from ml.inference import murmur3,predict
from storage.yelp import database

NUM_FEATURES=8192

def resume_publication(db,source,run_id,directory,model_root):
    from pyspark.sql import functions as F
    spark=session('sentiment-publication-resume');start=time.monotonic()
    try:
        pointer=json.loads(Path('/models/sentiment-active.json').read_text())
        predictor=json.loads((model_root/'predictor.json').read_text())
        checkpoint=directory/'training-checkpoint.json'
        if checkpoint.exists(): report=json.loads(checkpoint.read_text())
        else:
            reports=json.loads(Path('/scratch/recovered-model-metrics.json').read_text())
            base=spark.read.parquet(str(directory/'text-base')).select('business_id','split','label')
            distribution=[r.asDict() for r in base.groupBy('split','label').count().collect()]
            overlap=base.filter("split='train'").select('business_id').distinct().intersect(base.filter("split='test'").select('business_id').distinct()).count()
            assert overlap==0 and sum(r['count'] for r in distribution)==source['verification']['reviews']
            report={'run_id':run_id,'model_id':pointer['model_id'],'source_load':source['_id'],'reviews':source['verification']['reviews'],'distribution':distribution,'business_split':{kind:base.filter(F.col('split')==kind).select('business_id').distinct().count() for kind in ['train','test']},'business_overlap':overlap,'models':reports,'selected':predictor['model_type'],'selection_metric':'macro F1 on business-disjoint held-out test; comparison of two models','num_features':NUM_FEATURES,'idf_fit':'training businesses only','predictor_spark_parity_samples':30,'feature_source':'text only','class_order':['negative','neutral','positive'],'prediction_path':str(directory/'predictions'),'model_directory':str(model_root),'checkpoint_recovered_from_real_metrics_log':True}
            save(checkpoint,report)
        assert (Path(report['prediction_path'])/'_SUCCESS').exists()
        spark.read.parquet(report['prediction_path']).foreachPartition(write_predictions)
        assert db.reviews.count_documents({'sentiment_model':report['model_id']})==report['reviews']
        report['publication_resumed']=True;report['resume_seconds']=round(time.monotonic()-start,3)
        save(directory/'sentiment.json',report);save('/data/lifemap-e1/sentiment-active.json',report)
        db.model_runs.replace_one({'_id':report['model_id']},{'_id':report['model_id'],**report},upsert=True)
        print('SENTIMENT_COMPLETE',json.dumps(report),flush=True)
    finally: spark.stop();db.client.close()

def metrics(rows):
    matrix=[[0]*3 for _ in range(3)]
    for row in rows: matrix[int(row.label)][int(row.prediction)]=int(row['count'])
    per_class=[]
    for i,name in enumerate(['negative','neutral','positive']):
        tp=matrix[i][i];support=sum(matrix[i]);predicted=sum(row[i] for row in matrix)
        precision=tp/predicted if predicted else 0;recall=tp/support if support else 0
        per_class.append({'class':name,'support':support,'precision':precision,'recall':recall,'f1':2*precision*recall/(precision+recall) if precision+recall else 0})
    return {'confusion_matrix':matrix,'per_class':per_class,'macro_f1':sum(x['f1'] for x in per_class)/3,'accuracy':sum(matrix[i][i] for i in range(3))/sum(map(sum,matrix))}

def write_predictions(rows):
    from pymongo import UpdateOne
    db=database();batch=[]
    try:
        for row in rows:
            batch.append(UpdateOne({'_id':row.review_id},{'$set':{'sentiment':int(row.prediction),'sentiment_probabilities':list(row.probabilities),'sentiment_model':row.model_id}}))
            if len(batch)==500: db.reviews.bulk_write(batch,ordered=False);batch.clear()
        if batch: db.reviews.bulk_write(batch,ordered=False)
    finally: db.client.close()

def main():
    from pyspark import StorageLevel
    from pyspark.ml import Pipeline,PipelineModel
    from pyspark.ml.classification import LogisticRegression,NaiveBayes
    from pyspark.ml.feature import SQLTransformer,RegexTokenizer,StopWordsRemover,HashingTF,IDF
    from pyspark.sql.column import Column,_to_java_column
    from pyspark.sql import functions as F
    Path('/scratch/lifemap-e1.pid').write_text(str(os.getpid()))
    db=database();source=db.ingestion_runs.find_one({'kind':'yelp_load','status':'complete'})
    if not source: raise RuntimeError('Complete approved Yelp load is required')
    run_id='restaurants-'+source['_id'][:12]+'-e1-v1'
    directory=Path('/data/lifemap-e1')/run_id;directory.mkdir(parents=True,exist_ok=True)
    model_root=Path('/models')/run_id;model_root.mkdir(parents=True,exist_ok=True)
    report_path=directory/'sentiment.json'
    if report_path.exists() and (model_root/'spark_pipeline/metadata/_SUCCESS').exists():
        from ml.inference import load_active
        load_active();print('TRAINING_CACHE_HIT',flush=True);return
    if (model_root/'spark_pipeline/metadata/_SUCCESS').exists() and (directory/'predictions/_SUCCESS').exists():
        return resume_publication(db,source,run_id,directory,model_root)
    spark=session('sentiment');start=time.monotonic()
    try:
        # Fijar HashingTF real antes de exportar parametros a la API sin JVM.
        hashing=HashingTF(inputCol='limited_tokens',outputCol='tf',numFeatures=NUM_FEATURES)
        for word in ['a','ab','abc','abcd','restaurant','excellent','terrible','neutral']:
            assert hashing.indexOf(word)==murmur3(word)%NUM_FEATURES,word
        expected=source['verification']['reviews'];base_path=directory/'text-base'
        if not (base_path/'_SUCCESS').exists():
            secret=Path('/run/secrets/mongo_reader_password').read_text().strip()
            uri='mongodb://lifemap_reader:'+quote(secret,safe='')+'@mongodb:27017/lifemap?authSource=lifemap'
            frame=(spark.read.format('mongodb').schema('review_id string, business_id string, text string, stars double').option('connection.uri',uri).option('database','lifemap').option('collection','reviews').option('partitioner','com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner').load())
            frame=(frame.withColumn('label',F.when(F.col('stars')<=2,0.0).when(F.col('stars')==3,1.0).otherwise(2.0))
                   .withColumn('split',F.when(F.pmod(F.xxhash64('business_id',F.lit(42)),100)<80,'train').otherwise('test')).drop('stars'))
            frame.repartition(16).write.mode('errorifexists').parquet(str(base_path))
        base=spark.read.parquet(str(base_path))
        distribution=[r.asDict() for r in base.groupBy('split','label').count().collect()]
        assert sum(r['count'] for r in distribution)==expected
        train=base.filter("split='train'");test=base.filter("split='test'")
        overlap=train.select('business_id').distinct().intersect(test.select('business_id').distinct()).count()
        assert overlap==0
        business_split={kind:base.filter(F.col('split')==kind).select('business_id').distinct().count() for kind in ['train','test']}
        clean=SQLTransformer(statement="SELECT *, regexp_replace(lower(text), '[^a-z ]', ' ') AS clean_text FROM __THIS__")
        tokenizer=RegexTokenizer(inputCol='clean_text',outputCol='tokens',pattern='\\s+',minTokenLength=2)
        stops=StopWordsRemover(inputCol='tokens',outputCol='filtered',stopWords=StopWordsRemover.loadDefaultStopWords('english'))
        limit=SQLTransformer(statement='SELECT *, slice(filtered, 1, 256) AS limited_tokens FROM __THIS__')
        preprocessing=Pipeline(stages=[clean,tokenizer,stops,limit,hashing,IDF(inputCol='tf',outputCol='features',minDocFreq=5)]).fit(train)
        feature_path=directory/'features'
        if not (feature_path/'_SUCCESS').exists():
            preprocessing.transform(base).select('review_id','business_id','label','split','features').write.mode('errorifexists').parquet(str(feature_path))
        features=spark.read.parquet(str(feature_path)).persist(StorageLevel.DISK_ONLY)
        fits={};reports={}
        for name,estimator in [('naive_bayes',NaiveBayes(smoothing=1.0)),('logistic_regression',LogisticRegression(maxIter=12,regParam=0.1,elasticNetParam=0.0,maxBlockSizeInMB=1.0))]:
            print('TRAINING',name,flush=True);clock=time.monotonic()
            fitted=estimator.fit(features.filter("split='train'").select('label','features'))
            result=metrics(fitted.transform(features.filter("split='test'")).groupBy('label','prediction').count().collect())
            result['fit_and_evaluation_seconds']=round(time.monotonic()-clock,3)
            fits[name]=fitted;reports[name]=result
            print('MODEL_METRICS',json.dumps({name:result}),flush=True)
        chosen=max(reports,key=lambda name:reports[name]['macro_f1']);classifier=fits[chosen]
        model_id=run_id+'-'+chosen
        if chosen=='naive_bayes': weights=classifier.theta.toArray().tolist();intercepts=classifier.pi.toArray().tolist()
        else: weights=classifier.coefficientMatrix.toArray().tolist();intercepts=classifier.interceptVector.toArray().tolist()
        exported={'model_id':model_id,'model_type':chosen,'num_features':NUM_FEATURES,'stopwords':stops.getStopWords(),'idf':preprocessing.stages[-1].idf.toArray().tolist(),'weights':weights,'intercepts':intercepts,'max_tokens':256,'feature_source':'text only; stars used exclusively to derive label'}
        # Misma limpieza/tokenizacion/hashing/probabilidades en Spark y API.
        checks=[]
        samples=base.filter("split='test'").select('text').limit(30)
        native=classifier.transform(preprocessing.transform(samples))
        for row in native.select('text','prediction','probability').collect():
            result=predict(row.text,exported)
            assert result['label']==int(row.prediction)
            assert max(abs(a-b) for a,b in zip(result['probabilities'].values(),row.probability))<1e-6
            checks.append(True)
        PipelineModel(stages=preprocessing.stages+[classifier]).save(str(model_root/'spark_pipeline'))
        save(model_root/'predictor.json',exported)
        payload=(model_root/'predictor.json').read_bytes()
        save('/models/sentiment-active.json',{'model_id':model_id,'predictor':run_id+'/predictor.json','sha256':hashlib.sha256(payload).hexdigest(),'spark_pipeline':run_id+'/spark_pipeline'})
        prediction_path=directory/'predictions'
        if not (prediction_path/'_SUCCESS').exists():
            probability_array=Column(spark._jvm.org.apache.spark.ml.functions.vector_to_array(_to_java_column('probability'),'float64'))
            classifier.transform(features).select('review_id','business_id','label','prediction',probability_array.alias('probabilities')).withColumn('model_id',F.lit(model_id)).write.mode('errorifexists').parquet(str(prediction_path))
        report={'run_id':run_id,'model_id':model_id,'source_load':source['_id'],'reviews':expected,'distribution':distribution,'business_split':business_split,'business_overlap':overlap,'models':reports,'selected':chosen,'selection_metric':'macro F1 on business-disjoint held-out test; used to compare two models, not an independent final estimate','num_features':NUM_FEATURES,'idf_fit':'training businesses only','predictor_spark_parity_samples':len(checks),'feature_source':'text only','seconds':round(time.monotonic()-start,3),'class_order':['negative','neutral','positive'],'prediction_path':str(prediction_path),'model_directory':str(model_root),'completed_utc':datetime.now(timezone.utc).isoformat()}
        save(directory/'training-checkpoint.json',report)
        predictions=spark.read.parquet(str(prediction_path));predictions.foreachPartition(write_predictions)
        assert db.reviews.count_documents({'sentiment_model':model_id})==expected
        report['seconds']=round(time.monotonic()-start,3)
        save(report_path,report);save('/data/lifemap-e1/sentiment-active.json',report)
        db.model_runs.replace_one({'_id':model_id},{'_id':model_id,**report},upsert=True)
        print('SENTIMENT_COMPLETE',json.dumps(report),flush=True)
        features.unpersist()
    finally: spark.stop();db.client.close()

if __name__=='__main__': main()
