"""Kafka real y lag desde offsets de Spark: sin grupos ni commits Kafka propios."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time


def probes():
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer=KafkaConsumer(bootstrap_servers='kafka:9092',group_id=None,enable_auto_commit=False,
                    api_version_auto_timeout_ms=2000,request_timeout_ms=3000,
                    session_timeout_ms=2000,heartbeat_interval_ms=500)
        try:
            parts=consumer.partitions_for_topic('reviews') or set()
            if len(parts)<3:raise ValueError('Missing topic partitions')
            latest={str(k.partition):v for k,v in consumer.end_offsets([TopicPartition('reviews',p) for p in parts]).items()}
        finally:consumer.close()
        kafka={'status':'ok','topic':'reviews','partitions':len(parts),'end_offsets':latest}
    except Exception:
        return {'status':'unavailable'},{'status':'unavailable','consumer_lag_messages':None}
    try:
        state=json.loads(Path(os.environ.get('STREAM_STATUS_FILE','/checkpoints/production/status.json')).read_text())
        age=max(0,time.time()-state['updated_epoch']);lags={};positions={}
        for name,q in state['progress'].items():
            if not q:raise ValueError('No completed query progress')
            offsets=q['sources'][0]['endOffset']['reviews']
            positions[name]=offsets
            lags[name]=sum(max(0,v-offsets.get(p,0)) for p,v in latest.items())
        ready=state['active'] and age<120 and len(lags)==2
        streaming={'status':'ok' if ready else 'unavailable','consumer_lag_messages':max(lags.values()),
                   'lag_by_query':lags,'processed_offsets':positions,'heartbeat_age_seconds':round(age,2),
                   'sentiment_model_id':state['model_id'],'risk_model_id':state.get('risk_model_id'),
                   'watermark_event_time':state['progress']['production_windows'].get('eventTime',{}).get('watermark')}
    except Exception:
        streaming={'status':'unavailable','consumer_lag_messages':None}
    return kafka,streaming
