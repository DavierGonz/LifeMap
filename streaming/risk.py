"""Inferencia batch de riesgo dentro de foreachBatch; nunca entrena modelos."""
from datetime import datetime, timezone
from ml.risk import FEATURES, competitor_density, feature_values, probability
from streaming.producer import instant
from streaming.sinks import collections


def risk_names(scope):
    return ('risk_features','alerts') if scope=='production' else ('__e2_'+scope+'_risk_features','__e2_'+scope+'_alerts')


def recover_stars(db, source, row):
    raw=db.reviews.find_one({'_id':row['review_id'],'business_id':row['business_id'],'date':row['date']},{'stars':1,'text':1})
    if raw is None:return False
    if raw['text']!=row['text']:raise ValueError('Received review conflicts with immutable Yelp content')
    db[source].update_one({'_id':row['review_id']},{'$set':{'stars':float(raw['stars'])}})
    return True


def ensure_risk_indexes(db, scope):
    features,alerts=risk_names(scope)
    for name in [features,alerts]: db[name].create_index([('geometry','2dsphere')],name='geometry_2dsphere')
    db[alerts].create_index([('business_id',1),('event_date',1)],name='business_event')
    source=collections(scope)[0]
    db[source].create_index([('business_id',1),('date',1)],name='business_date')
    db[source].create_index([('date',1),('business_id',1)],name='date_business')
    # Upgrade only messages already received before risk inference was enabled.
    # Ratings belong to those immutable events; no unseen/future review is scanned.
    for row in db[source].find({'stars':{'$exists':False}}, {'review_id':1,'business_id':1,'date':1,'text':1}):
        recover_stars(db,source,row)


def update_risk(db, rows, scope, assets, spark):
    """As-of = fecha del evento. Solo baseline <=T y mensajes ya recibidos <=as-of."""
    if not rows:return
    model,manifest,native=assets
    source=collections(scope)[0];feature_name,alert_name=risk_names(scope)
    version=model['model_id'];cutoff=instant(model['cutoff'])
    catalog_meta=db.risk_metadata.find_one({'_id':'catalog'})
    assert catalog_meta['sha256']==model['catalog_sha256'], 'Frozen geography/category catalog changed'
    eligible=[];seen=set()
    for r in rows:
        if r['review_id'] in seen:continue
        seen.add(r['review_id'])
        # Rating belongs to this received review. Never read business.stars or an
        # unreceived review to update the historical rating. Old Kafka schema stays valid.
        if not recover_stars(db,source,r):continue  # Synthetic fixtures cannot invent Yelp ratings.
        eligible.append(r)
    feature_rows=[];metadata={}
    for r in sorted(eligible,key=lambda x:(x['date'],x['review_id'])):
        existing=db[source].find_one({'_id':r['review_id']},{'risk_processed_models':1})
        if version in existing.get('risk_processed_models',[]):continue
        id=r['business_id'];asof=instant(r['date'])
        baseline=db.risk_baselines.find_one({'_id':id})
        if baseline:
            assert instant(baseline['as_of'])==cutoff and baseline['sentiment_model_id']==model['sentiment_model_id']
        business=db.risk_catalog.find_one({'_id':id})
        assert business and instant(business['first_review'])<=asof
        history=list(db[source].find({'business_id':id,'date':{'$gt':cutoff,'$lte':asof},'stars':{'$exists':True}},
                          {'_id':0,'review_id':1,'date':1,'prediction':1,'stars':1}))
        density=competitor_density(db,business,asof,scope,cutoff)
        values,quality=feature_values(baseline,history,asof,density,cutoff)
        feature_rows.append({'review_id':r['review_id'],**values})
        metadata[r['review_id']]={'business_id':id,'asof':asof,'values':values,'quality':quality,'geometry':business['location']}
    if not feature_rows:return
    # The MLlib PipelineModel is loaded once at consumer startup, distinct from
    # the batch training entrypoint. Python coefficients are only a parity check.
    scored=native.transform(spark.createDataFrame(feature_rows)).select('review_id','probability')
    probabilities={r.review_id:float(r.probability[1]) for r in scored.toLocalIterator()}
    for row in feature_rows:
        rid=row['review_id'];m=metadata[rid];id=m['business_id'];asof=m['asof'];p=probabilities[rid]
        assert abs(p-probability(m['values'],model))<1e-8, 'Risk artifact does not match native MLlib'
        previous=db[feature_name].find_one({'_id':id},{'as_of':1})
        advances=previous is None or instant(previous['as_of'])<=asof
        now=datetime.now(timezone.utc)
        state={'business_id':id,'as_of':asof,'features':m['values'],'quality':m['quality'],
               'probability':p,'threshold':model['threshold'],'geometry':m['geometry'],
               'model_id':version,'model_sha256':manifest['sha256'],'processed_at':now,
               'retrospective_simulation_only':True,'uncalibrated_score':True}
        if advances:
            db[feature_name].replace_one({'_id':id},{'_id':id,**state},upsert=True)
            db[alert_name].update_many({'business_id':id,'model_id':version,'event_date':{'$lte':asof},'active':True},
                                      {'$set':{'active':False}})
        if p>=model['threshold']:
            key=version+':'+rid
            alert={**state,'review_id':rid,'event_date':asof,'test_fixture':scope!='production',
                   'late_relative_to_current_state':not advances}
            db[alert_name].update_one({'_id':key},{'$setOnInsert':alert,'$set':{'active':advances}},upsert=True)
        # Mark only after all effects. A partial failure redoes deterministic upserts;
        # a completed message is never rescored after more historical messages arrive.
        db[source].update_one({'_id':rid},{'$addToSet':{'risk_processed_models':version}})
