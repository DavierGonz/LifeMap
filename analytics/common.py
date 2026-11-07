"""Adaptacion de afr/rdd_counts/df_counts de Discos, solo para LifeMap."""
import json
import math
from pathlib import Path

MIN_BUSINESS_DAYS=3650.0  # diez anos-negocio agregados, no diez negocios.
def closure_rate(closed,days,minimum=MIN_BUSINESS_DAYS):
    if closed<0 or days<0: raise ValueError('Negative counts')
    return closed/(days/365)*100 if days>0 and days>=minimum else None

def rdd_counts(frame,key,partitions):
    return dict(frame.select(key,'event','business_days').rdd.map(lambda r:(r[0],(int(r[1]),float(r[2])))).reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]),numPartitions=partitions).collect())

def df_counts(frame,key):
    from pyspark.sql import functions as F
    return {r[0]:(int(r[1]),float(r[2])) for r in frame.groupBy(key).agg(F.sum('event'),F.sum('business_days')).collect()}

def equal(left,right):
    return left.keys()==right.keys() and all(left[k][0]==right[k][0] and math.isclose(left[k][1],right[k][1],rel_tol=1e-10,abs_tol=1e-6) for k in left)

def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(value,indent=2,ensure_ascii=False,default=str),encoding='utf-8');temporary.replace(path)

def session(name):
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.appName('lifemap-'+name).config('spark.sql.adaptive.enabled','false').config('spark.sql.shuffle.partitions','8').config('spark.sql.session.timeZone','UTC').getOrCreate())
    spark.sparkContext.setLogLevel('WARN');return spark
