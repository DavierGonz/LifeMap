"""Los seis endpoints academicos de Entrega 1."""
import os
import re
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, jsonify,request
from werkzeug.exceptions import BadRequest,HTTPException,ServiceUnavailable
from pymongo.errors import PyMongoError
from functools import lru_cache
from pathlib import Path
from api import repository,validation
from analytics.common import closure_rate,MIN_BUSINESS_DAYS
from ml.inference import load_active,predict

from api.probes import dask_probe, mongo_probe, spark_probe


def create_app():
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 32 * 1024
    repository.configure(app)

    def category(value):
        if value is None: return None
        if not isinstance(value,str) or not value.strip() or len(value)>128:
            raise BadRequest('category must be a nonempty string up to 128 characters')
        return value.strip()

    @app.errorhandler(HTTPException)
    def bad_request(error): return jsonify(error=error.name,message=error.description),error.code

    @app.errorhandler(PyMongoError)
    def database_error(error): return jsonify(error='Service Unavailable',message='Database query unavailable'),503

    @app.errorhandler(Exception)
    def internal_error(error):
        app.logger.exception('LifeMap request failed')
        return jsonify(error='Internal Server Error',message='Request could not be completed'),500

    @app.get('/businesses/near')
    def near():
        point,radius,limit=validation.proximity(request.args)
        rows=repository.near(point,radius,limit,category(request.args.get('category')))
        return jsonify(count=len(rows),limit=limit,businesses=repository.serializable(rows))

    @app.post('/businesses/within')
    def within():
        body=request.get_json();geometry,limit=validation.polygon(body)
        rows=repository.within(geometry,limit,category(body.get('category')))
        return jsonify(count=len(rows),limit=limit,businesses=repository.serializable(rows))

    @app.get('/zones/summary')
    def zone():
        point,radius,_=validation.proximity(request.args)
        row=repository.zone(point,radius,category(request.args.get('category')))
        row['rate_pct']=closure_rate(row['closures'],row['business_days'])
        row['minimum_business_days']=MIN_BUSINESS_DAYS
        row['negative_pct']=row['negative_reviews']/row['reviews']*100 if row['reviews'] else None
        row['mean_sentiment']=(row['positive_reviews']-row['negative_reviews'])/row['reviews'] if row['reviews'] else None
        return jsonify(row)

    @app.get('/stats/closure')
    def closure():
        validation.arguments(request.args,('group_by','min_business_days','limit'))
        kind=request.args.get('group_by','category')
        if kind not in ('category','geohash'): raise BadRequest('group_by must be category or geohash')
        minimum=validation.number(request.args.get('min_business_days',MIN_BUSINESS_DAYS),'min_business_days',1,1e12)
        limit=validation.integer(request.args.get('limit',20),'limit',1,100)
        rows=repository.closure(kind,minimum,limit)
        for row in rows:
            row['rate_pct']=closure_rate(row['closures'],row['business_days'],minimum)
            row['minimum_business_days']=minimum
        return jsonify(group_by=kind,minimum_business_days=minimum,count=len(rows),results=repository.serializable(rows))

    @lru_cache(maxsize=1)
    def current_model(modified): return load_active()

    @app.post('/predict')
    def prediction():
        body=request.get_json()
        if not isinstance(body,dict) or set(body)!={'text'} or not isinstance(body['text'],str) or not body['text'].strip() or len(body['text'])>10000:
            raise BadRequest('Expected nonempty text, up to 10000 characters')
        try: model=current_model(Path('/models/sentiment-active.json').stat().st_mtime_ns)
        except (OSError,ValueError): raise ServiceUnavailable('Trained model is not available') from None
        return jsonify(predict(body['text'],model))

    @app.get("/live")
    def live():
        return jsonify(service="lifemap-api", status="ok", scope="local_infrastructure")

    @app.get("/health")
    def health():
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                "mongodb": executor.submit(mongo_probe),
                "spark": executor.submit(spark_probe),
                "dask": executor.submit(dask_probe),
            }
            services = {name: future.result() for name, future in futures.items()}
        services["api"] = {"status": "ok"}
        ready = all(value["status"] == "ok" for value in services.values())
        commit = os.environ.get("DEPLOYED_COMMIT", "unreleased")
        return jsonify(
            service="lifemap-api", status="ok" if ready else "degraded",
            scope="delivery_1", services=services, commit=commit,
            commit_configured=bool(re.fullmatch(r"[0-9a-f]{40}", commit)),
        ), 200 if ready else 503

    return app
