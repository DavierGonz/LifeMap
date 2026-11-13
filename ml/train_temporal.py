"""Correccion batch del pipeline E1: corpus y seleccion <= T, artefacto separado."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote

from analytics.common import save, session
from ml.inference import predict
from ml.train import NUM_FEATURES, metrics


def main():
    from pyspark import StorageLevel
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.classification import LogisticRegression, NaiveBayes
    from pyspark.ml.feature import SQLTransformer, RegexTokenizer, StopWordsRemover, HashingTF, IDF
    from pyspark.sql import functions as F
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cutoff', required=True)
    args = p.parse_args()
    cutoff = datetime.fromisoformat(args.cutoff.replace('Z', '+00:00'))
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    cutoff = cutoff.astimezone(timezone.utc)
    root = Path('/models')
    original = (root/'sentiment-active.json').read_bytes()
    original_sha = hashlib.sha256(original).hexdigest()
    version = 'sentiment-until-' + cutoff.strftime('%Y%m%dT%H%M%SZ') + '-v1'
    model_dir = root / 'e2-temporal' / version
    data_dir = Path('/data/lifemap-e2') / version
    report_path = data_dir/'training.json'
    if report_path.exists():
        report = json.loads(report_path.read_text())
        manifest = json.loads((root/'sentiment-temporal.json').read_text())
        assert manifest['model_id'] == report['model_id']
        assert hashlib.sha256((root/manifest['predictor']).read_bytes()).hexdigest() == manifest['sha256']
        assert (root/'sentiment-active.json').read_bytes() == original
        print('TEMPORAL_MODEL_CACHE_HIT', json.dumps(report), flush=True)
        return
    if model_dir.exists():
        raise RuntimeError('Partial artifact exists; inspect before retry, never overwrite automatically')
    data_dir.mkdir(parents=True, exist_ok=True)
    spark = session('sentiment-temporal')
    started = time.monotonic()
    try:
        secret = Path('/run/secrets/mongo_reader_password').read_text().strip()
        uri = 'mongodb://lifemap_reader:' + quote(secret, safe='') + '@mongodb:27017/lifemap?authSource=lifemap'
        frame = (spark.read.format('mongodb')
                 .schema('review_id string, business_id string, date timestamp, text string, stars double')
                 .option('connection.uri', uri).option('database', 'lifemap').option('collection', 'reviews')
                 .option('partitioner', 'com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner').load()
                 .filter(F.col('date') <= F.lit(cutoff.replace(tzinfo=None))))
        base = (frame.withColumn('label', F.when(F.col('stars') <= 2, 0.0).when(F.col('stars') == 3, 1.0).otherwise(2.0))
                .withColumn('split', F.when(F.pmod(F.xxhash64('business_id', F.lit(42)), 100) < 80, 'train').otherwise('test'))
                .drop('stars'))
        # Materializacion independiente de E1, reutilizable; nunca sobrescribe sus features.
        base_path = data_dir/'base'
        if not (base_path/'_SUCCESS').exists():
            base.repartition(16).write.mode('errorifexists').parquet(str(base_path))
        base = spark.read.parquet(str(base_path))
        distribution = [r.asDict() for r in base.groupBy('split', 'label').count().collect()]
        train, test = base.filter("split='train'"), base.filter("split='test'")
        train_max = train.agg(F.max('date')).first()[0]
        selection_max = test.agg(F.max('date')).first()[0]
        assert train_max <= cutoff.replace(tzinfo=None) and selection_max <= cutoff.replace(tzinfo=None)
        overlap = train.select('business_id').distinct().intersect(test.select('business_id').distinct()).count()
        assert overlap == 0
        proof = hashlib.sha256()
        training_count = 0
        for row in train.select('review_id').orderBy('review_id').toLocalIterator():
            proof.update((row.review_id + '\n').encode('utf-8'))
            training_count += 1
        clean = SQLTransformer(statement="SELECT *, regexp_replace(lower(text), '[^a-z ]', ' ') AS clean_text FROM __THIS__")
        tokenizer = RegexTokenizer(inputCol='clean_text', outputCol='tokens', pattern='\\s+', minTokenLength=2)
        stops = StopWordsRemover(inputCol='tokens', outputCol='filtered', stopWords=StopWordsRemover.loadDefaultStopWords('english'))
        limit = SQLTransformer(statement='SELECT *, slice(filtered, 1, 256) AS limited_tokens FROM __THIS__')
        hashing = HashingTF(inputCol='limited_tokens', outputCol='tf', numFeatures=NUM_FEATURES)
        preprocessing = Pipeline(stages=[clean, tokenizer, stops, limit, hashing,
                                         IDF(inputCol='tf', outputCol='features', minDocFreq=5)]).fit(train)
        feature_path = data_dir/'features'
        if not (feature_path/'_SUCCESS').exists():
            preprocessing.transform(base).select('review_id', 'business_id', 'label', 'split', 'features').write.mode('errorifexists').parquet(str(feature_path))
        features = spark.read.parquet(str(feature_path)).persist(StorageLevel.DISK_ONLY)
        reports, fits = {}, {}
        for name, estimator in [('naive_bayes', NaiveBayes(smoothing=1.0)),
                                ('logistic_regression', LogisticRegression(maxIter=12, regParam=0.1,
                                 elasticNetParam=0.0, maxBlockSizeInMB=1.0))]:
            fitted = estimator.fit(features.filter("split='train'").select('label', 'features'))
            reports[name] = metrics(fitted.transform(features.filter("split='test'")).groupBy('label', 'prediction').count().collect())
            fits[name] = fitted
            print('TEMPORAL_METRICS', json.dumps({name: reports[name]}), flush=True)
        chosen = max(reports, key=lambda n: reports[n]['macro_f1'])
        classifier = fits[chosen]
        model_id = version + '-' + chosen
        if chosen == 'naive_bayes':
            weights, intercepts = classifier.theta.toArray().tolist(), classifier.pi.toArray().tolist()
        else:
            weights, intercepts = classifier.coefficientMatrix.toArray().tolist(), classifier.interceptVector.toArray().tolist()
        exported = dict(model_id=model_id, model_type=chosen, num_features=NUM_FEATURES,
                        stopwords=stops.getStopWords(), idf=preprocessing.stages[-1].idf.toArray().tolist(),
                        weights=weights, intercepts=intercepts, max_tokens=256,
                        feature_source='text only; stars derive label; all rows <= cutoff')
        checks = 0
        native = classifier.transform(preprocessing.transform(test.select('text').limit(30)))
        for row in native.select('text', 'prediction', 'probability').collect():
            result = predict(row.text, exported)
            assert result['label'] == int(row.prediction)
            assert max(abs(a-b) for a, b in zip(result['probabilities'].values(), row.probability)) < 1e-6
            checks += 1
        model_dir.mkdir(parents=True)
        PipelineModel(stages=preprocessing.stages + [classifier]).save(str(model_dir/'spark_pipeline'))
        save(model_dir/'predictor.json', exported)
        manifest = dict(model_id=model_id, cutoff=cutoff.isoformat(), temporal_training_verified=True,
                        training_max_date=train_max.replace(tzinfo=timezone.utc).isoformat(),
                        selection_max_date=selection_max.replace(tzinfo=timezone.utc).isoformat(),
                        training_review_ids_sha256=proof.hexdigest(), training_reviews=training_count,
                        predictor=str((model_dir/'predictor.json').relative_to(root)),
                        spark_pipeline=str((model_dir/'spark_pipeline').relative_to(root)),
                        sha256=hashlib.sha256((model_dir/'predictor.json').read_bytes()).hexdigest())
        assert (root/'sentiment-active.json').read_bytes() == original
        report = {**manifest, 'distribution': distribution, 'business_overlap': overlap,
                  'models': reports, 'selected': chosen, 'parity_samples': checks,
                  'selection_metric': 'macro F1 on business-disjoint pre-T holdout; not independent final test',
                  'E1_active_pointer_sha256_unchanged': original_sha,
                  'seconds': round(time.monotonic()-started, 3), 'mongo_writes': 0}
        save(report_path, report)
        save(root/'sentiment-temporal.json', manifest)
        print('TEMPORAL_MODEL_COMPLETE', json.dumps(report), flush=True)
        features.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
