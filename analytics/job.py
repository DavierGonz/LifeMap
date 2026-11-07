"""RDD/groupBy, tasas y 24 mediciones reales; adaptado de Discos batch.py."""
from collections import defaultdict
import csv
import json
import os
from pathlib import Path
import statistics
import time

from analytics.common import MIN_BUSINESS_DAYS,closure_rate,rdd_counts,df_counts,equal,save,session
from analytics.geo import geohash,geometry
from storage.yelp import database,main_category

def main():
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from pymongo import UpdateOne,ReplaceOne
    Path('/scratch/lifemap-e1.pid').write_text(str(os.getpid()))
    model=json.loads(Path('/data/lifemap-e1/sentiment-active.json').read_text())
    directory=Path('/data/lifemap-e1')/model['run_id'];report_path=directory/'analytics.json'
    if report_path.exists(): print('ANALYTICS_CACHE_HIT',flush=True);return
    db=database();spark=session('closure-benchmark');start=time.monotonic()
    try:
        input_path=directory/'businesses.jsonl';updates=[];business_counts=defaultdict(int)
        with input_path.open('w',encoding='utf-8') as stream:
            for row in db.businesses.find({}, {'business_id':1,'city':1,'categories':1,'latitude':1,'longitude':1,'event':1,'business_days':1}):
                category=main_category(row['categories']);cell=geohash(row['latitude'],row['longitude'],5)
                document={key:row.get(key) for key in ['business_id','city','event','business_days']}
                document.update(category_primary=category,geohash=cell)
                stream.write(json.dumps(document)+'\n')
                updates.append(UpdateOne({'_id':row['_id']},{'$set':{'category_primary':category,'category_primary_rule':'alphabetically first non-family Yelp tag; not an official Yelp primary category','geohash':cell}}))
                if len(updates)==500: db.businesses.bulk_write(updates,ordered=False);updates.clear()
        if updates: db.businesses.bulk_write(updates,ordered=False)
        raw=spark.read.schema('business_id string, city string, event integer, business_days double, category_primary string, geohash string').json(str(input_path))
        frame=raw.filter('business_days IS NOT NULL AND business_days >= 0').persist(StorageLevel.MEMORY_AND_DISK)
        total=frame.count();assert total==16944
        keys=['category_primary','geohash'];references={key:df_counts(frame,key) for key in keys}
        for key in keys: assert equal(rdd_counts(frame,key,8),references[key])
        predictions=spark.read.parquet(model['prediction_path'])
        business_sentiment=predictions.groupBy('business_id').agg(F.count('*').alias('review_count'),F.sum(F.when(F.col('prediction')==0,1).otherwise(0)).alias('negative_count'),F.sum(F.when(F.col('prediction')==2,1).otherwise(0)).alias('positive_count'))
        updates=[]
        for row in business_sentiment.toLocalIterator():
            summary={'reviews':int(row.review_count),'negative':int(row.negative_count),'positive':int(row.positive_count),'mean':float(row.positive_count-row.negative_count)/row.review_count,'model_id':model['model_id']}
            updates.append(UpdateOne({'_id':row.business_id},{'$set':{'sentiment_summary':summary}}))
            if len(updates)==500: db.businesses.bulk_write(updates,ordered=False);updates.clear()
        if updates: db.businesses.bulk_write(updates,ordered=False)
        cell_sentiment={r.geohash:(int(r.reviews),int(r.negative)) for r in business_sentiment.join(frame.select('business_id','geohash'),'business_id').groupBy('geohash').agg(F.sum('review_count').alias('reviews'),F.sum('negative_count').alias('negative')).collect()}
        rows=[]
        for key in keys:
            sizes={r[0]:int(r[1]) for r in frame.groupBy(key).count().collect()}
            for group,(closed,days) in sorted(references[key].items()):
                kind='category' if key=='category_primary' else 'geohash'
                row={'_id':model['run_id']+'|'+kind+'|'+group,'run_id':model['run_id'],'kind':kind,'key':group,'closures':closed,'business_days':days,'businesses':sizes[group],'rate_pct':closure_rate(closed,days),'minimum_business_days':MIN_BUSINESS_DAYS,'rdd_dataframe_equal':True}
                if key=='geohash':
                    n,negative=cell_sentiment.get(group,(0,0));row.update(geometry=geometry(group),reviews=n,negative_reviews=negative,negative_pct=negative/n*100 if n else None)
                rows.append(row)
        for offset in range(0,len(rows),500): db.closure_stats.bulk_write([ReplaceOne({'_id':r['_id']},r,upsert=True) for r in rows[offset:offset+500]],ordered=False)
        benchmark=[];distribution=[];dispersion=[]
        for n in [2,8,32,200]:
            for mode in ['city','round_robin']:
                repartitioned=frame.repartition(n,'city') if mode=='city' else frame.repartition(n)
                # Contar tambien particiones vacias; AQE desactivado conserva n.
                sizes={int(r.pid):int(r['count']) for r in repartitioned.withColumn('pid',F.spark_partition_id()).groupBy('pid').count().collect()}
                values=[sizes.get(i,0) for i in range(n)]
                distribution.extend({'partitions':n,'mode':mode,'partition_id':i,'rows':v} for i,v in enumerate(values))
                for key in keys: assert equal(rdd_counts(repartitioned,key,n),references[key])
                for repetition in [1,2,3]:
                    clock=time.perf_counter()
                    for key in keys: assert equal(rdd_counts(repartitioned,key,n),references[key])
                    item={'partitions':n,'mode':mode,'repetition':repetition,'seconds':time.perf_counter()-clock};benchmark.append(item)
                    save(directory/'benchmark-progress.json',benchmark);print('BENCHMARK',json.dumps(item),flush=True)
                measured=[r['seconds'] for r in benchmark if r['partitions']==n and r['mode']==mode]
                dispersion.append({'partitions':n,'mode':mode,'mean_seconds':statistics.mean(measured),'median_seconds':statistics.median(measured),'stdev_seconds':statistics.stdev(measured),'min_seconds':min(measured),'max_seconds':max(measured),'empty_partitions':values.count(0),'max_partition_rows':max(values),'min_partition_rows':min(values),'max_over_mean_rows':max(values)/statistics.mean(values)})
        for name,data in [('benchmark-times',benchmark),('benchmark-summary',dispersion),('partition-distribution',distribution)]:
            with (directory/(name+'.csv')).open('w',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=list(data[0]));writer.writeheader();writer.writerows(data)
        report={'run_id':model['run_id'],'model_id':model['model_id'],'businesses':total,'reviews':model['reviews'],'rdd_dataframe_equal':True,'groups':{'category':len(references['category_primary']),'geohash':len(references['geohash'])},'minimum_business_days':MIN_BUSINESS_DAYS,'minimum_justification':'ten aggregated business-years to avoid ranking near-zero-exposure groups; operational threshold, not a confidence interval','closure_formula':'closures / (business_days / 365) * 100','category_rule':'alphabetically first non-family source tag; population remains Restaurants','geohash_precision':5,'benchmark_times':benchmark,'benchmark_summary':dispersion,'partition_distribution':distribution,'benchmark_operation':'both category and geohash RDD reductions with collection and independent DataFrame equality checks; repartition included','benchmark_source':'16944 observed business lifetimes, MEMORY_AND_DISK materialized before timing','warmup':'one unmeasured pair of reductions per partition/mode configuration','spark':spark.version,'master':spark.sparkContext.master,'seconds':round(time.monotonic()-start,3),'total_closures':sum(v[0] for v in references['geohash'].values()),'total_business_days':sum(v[1] for v in references['geohash'].values())}
        save(report_path,report);save('/data/lifemap-e1/analytics-active.json',report)
        db.analytics_runs.replace_one({'_id':model['run_id']},{'_id':model['run_id'],'status':'complete',**report},upsert=True)
        db.analytics_state.replace_one({'_id':'active'},{'_id':'active','run_id':model['run_id'],'model_id':model['model_id']},upsert=True)
        print('ANALYTICS_COMPLETE',json.dumps({k:v for k,v in report.items() if k not in {'benchmark_times','partition_distribution'}}),flush=True)
    finally: spark.stop();db.client.close()

if __name__=='__main__': main()
