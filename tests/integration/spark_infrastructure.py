"""Job sintetico minimo y carga del conector; sin consultar MongoDB."""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('lifemap-infrastructure-check').getOrCreate()
try:
    spark.sparkContext.setLogLevel('ERROR')
    connector = spark._jvm.java.lang.Class.forName('com.mongodb.spark.sql.connector.MongoTableProvider').getName()
    result = spark.range(10).groupBy().sum('id').first()[0]
    assert result == 45
    assert spark.sparkContext.master == 'spark://spark-master-internal:7077'
    print(json.dumps({'sum': result, 'connector': connector, 'spark_version': spark.version, 'status': 'passed'}))
finally:
    spark.stop()
