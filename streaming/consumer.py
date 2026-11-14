"""Dos sinks Structured Streaming: inferencia completa y ventanas finalizadas."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from analytics.common import save, session
from analytics.geo import geohash
from streaming.producer import instant, validate_model
from streaming.sinks import (bind_checkpoint, collections, ensure_indexes,
                             pipeline_hash, save_reviews, save_windows, writer)


def main():
    from pyspark.ml import PipelineModel
    from pyspark.sql.column import Column, _to_java_column
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--scope', default='production')
    p.add_argument('--checkpoint-root', default='/checkpoints')
    p.add_argument('--available-now', action='store_true')
    p.add_argument('--simulate-sink-failure', action='store_true')
    args = p.parse_args()
    collections(args.scope)
    if args.simulate_sink_failure and args.scope == 'production':
        raise ValueError('Test fault injection is not allowed for production')
    config = json.loads(Path('/opt/lifemap/config/streaming.json').read_text())
    manifest = json.loads(Path(config['temporal_manifest']).read_text())
    assert validate_model(manifest, config['cutoff'], '/models') == config['model_id']
    assert manifest['sha256'] == config['model_sha256'], 'Unversioned sentiment model'
    pipeline = (Path('/models')/manifest['spark_pipeline']).resolve()
    if not pipeline.is_relative_to(Path('/models')):
        raise ValueError('Spark model must remain inside model volume')
    checkpoint = Path(args.checkpoint_root)/args.scope
    fingerprint = pipeline_hash(pipeline)
    assert fingerprint == config['spark_pipeline_sha256'], 'Saved Spark pipeline differs from versioned model'
    binding = {'model_id': manifest['model_id'], 'predictor_sha256': manifest['sha256'],
               'spark_pipeline_sha256': fingerprint, 'config': config, 'shuffle_partitions': 2}
    bind_checkpoint(checkpoint, binding)
    db = writer()
    try:
        ensure_indexes(db, args.scope)
        metadata = []
        for b in db.businesses.find({}, {'business_id': 1, 'location': 1, '_id': 0}):
            lon, lat = b['location']['coordinates']
            metadata.append((b['business_id'], float(lon), float(lat), geohash(lat, lon, config['geohash_precision'])))
        assert len(metadata) == 16944, 'E1 business corpus changed'
    finally:
        db.client.close()
    spark = session('structured-streaming')
    queries = []
    try:
        model = PipelineModel.load(str(pipeline))  # LOAD only; no fit/train anywhere in this consumer.
        businesses = spark.createDataFrame(metadata, 'business_id string, longitude double, latitude double, geohash string')
        schema = StructType([StructField(k, StringType()) for k in
                             ['review_id', 'business_id', 'text', 'date', 'model_id', 'replay_id']]
                            + [StructField('test_fixture', BooleanType()), StructField('schema_version', IntegerType())])
        kafka = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092')
                 .option('subscribe', 'reviews').option('startingOffsets', 'earliest')
                 .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', 100).load())
        data = kafka.select(F.from_json(F.col('value').cast('string'), schema).alias('r'),
                            F.col('key').cast('string').alias('kafka_key'), 'topic', 'partition', 'offset')
        data = data.select('r.*', 'kafka_key', 'topic', 'partition', 'offset')
        if args.scope == 'production':
            data = data.filter(~F.coalesce(F.col('test_fixture'), F.lit(False)))
        else:
            data = data.filter((F.col('test_fixture') == True) & (F.col('replay_id') == args.scope))
        data = (data.withColumnRenamed('date', 'date_original')
                .withColumn('date', F.to_timestamp('date_original'))
                .filter((F.col('date') > F.lit(instant(config['cutoff']).replace(tzinfo=None))) &
                        (F.col('kafka_key') == F.col('business_id')) &
                        (F.col('model_id') == manifest['model_id']) & (F.col('schema_version') == 1) &
                        (F.length(F.trim('text')) > 0) & (F.length('review_id') > 0))
                .join(F.broadcast(businesses), 'business_id'))
        probability_array = Column(spark._jvm.org.apache.spark.ml.functions.vector_to_array(
            _to_java_column('probability'), 'float64'))
        classified = model.transform(data).select('review_id', 'business_id', 'text', 'date', 'date_original',
                    'longitude', 'latitude', 'geohash', 'topic', 'partition', 'offset', 'prediction',
                    probability_array.alias('probabilities'))
        # Independent sinks: late events remain classified; only windows apply late-data policy.
        def review_sink(frame, batch_id):
            save_reviews(frame, batch_id, args.scope, manifest, checkpoint, args.simulate_sink_failure)
        deduped = classified.withWatermark('date', '7 days').dropDuplicatesWithinWatermark(['review_id'])
        anchor_days = (instant(config['cutoff']) - instant('1970-01-01')).days % 30
        windows = deduped.groupBy('geohash', F.window('date', '30 days', '30 days', f'{anchor_days} days')).agg(
            F.count('*').alias('reviews'), F.sum(F.when(F.col('prediction') == 0, 1).otherwise(0)).alias('negative'))
        def window_sink(frame, batch_id):
            save_windows(frame, batch_id, args.scope, manifest, config)
        for name, frame, callback in [('reviews', classified, review_sink), ('windows', windows, window_sink)]:
            sink = (frame.writeStream.queryName(args.scope+'_'+name).outputMode('append')
                    .option('checkpointLocation', str(checkpoint/name)).foreachBatch(callback))
            sink = sink.trigger(availableNow=True) if args.available_now else sink.trigger(processingTime='10 seconds')
            query = sink.start()
            queries.append(query)
            if args.available_now:
                query.awaitTermination()  # Sequential runs reduce peak RAM; offsets persist independently.
        def status(active):
            progress = {q.name: q.lastProgress for q in queries}
            save(checkpoint/'status.json', {'active': active, 'scope': args.scope,
                 'model_id': manifest['model_id'], 'updated_epoch': time.time(), 'progress': progress,
                 'recent': {q.name: q.recentProgress for q in queries}})
        if args.available_now:
            status(False)
            print('STREAMING_COMPLETE', json.dumps({'scope': args.scope, 'model_id': manifest['model_id']}), flush=True)
        else:
            while all(q.isActive for q in queries):
                status(True)
                time.sleep(5)
            for q in queries:
                if q.exception():
                    raise q.exception()
            raise RuntimeError('A streaming query stopped unexpectedly')
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()


if __name__ == '__main__':
    main()
