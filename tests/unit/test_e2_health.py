"""Lag de dos consultas: offsets consumidos, heartbeat y Kafka inaccesible."""
from collections import namedtuple
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('stream_health',ROOT/'api/stream_health.py')
health=importlib.util.module_from_spec(spec);spec.loader.exec_module(health)
TP=namedtuple('TopicPartition','topic partition')


class Consumer:
    def __init__(self,**kw):
        assert kw['group_id'] is None and kw['enable_auto_commit'] is False
    def partitions_for_topic(self,topic):return {0,1,2}
    def end_offsets(self,parts):return {p:[10,20,30][p.partition] for p in parts}
    def close(self):pass


class StreamingHealthTests(unittest.TestCase):
    def state(self):
        query=lambda offsets:dict(sources=[dict(endOffset={'reviews':dict(zip(['0','1','2'],offsets))})],eventTime={'watermark':'2020-01-01T00:00:00Z'})
        return dict(updated_epoch=100,active=True,model_id='temporal-model',risk_model_id='risk-v1',
                    progress=dict(production_reviews=query([9,18,30]),production_windows=query([10,20,25])))

    def run_probe(self,state,consumer=Consumer):
        module=SimpleNamespace(KafkaConsumer=consumer,TopicPartition=TP)
        with patch.dict(sys.modules,{'kafka':module}),patch.object(health.Path,'read_text',return_value=json.dumps(state)),patch.object(health.time,'time',return_value=101):
            return health.probes()

    def test_independent_consumers_use_maximum_not_sum_and_three_partitions(self):
        broker,stream=self.run_probe(self.state())
        self.assertEqual(broker['partitions'],3)
        self.assertEqual(stream['lag_by_query'],{'production_reviews':3,'production_windows':5})
        self.assertEqual(stream['consumer_lag_messages'],5)
        self.assertEqual(stream['status'],'ok')

    def test_historical_event_time_is_not_wall_clock_lag(self):
        _,stream=self.run_probe(self.state())
        self.assertEqual(stream['heartbeat_age_seconds'],1)
        self.assertEqual(stream['watermark_event_time'],'2020-01-01T00:00:00Z')

    def test_stale_or_incomplete_progress_is_unavailable(self):
        state=self.state();state['updated_epoch']=-100
        self.assertEqual(self.run_probe(state)[1]['status'],'unavailable')
        state=self.state();state['progress']['production_reviews']=None
        self.assertIsNone(self.run_probe(state)[1]['consumer_lag_messages'])

    def test_broker_failure_has_no_fabricated_zero_lag(self):
        def unavailable(**kw):raise ConnectionError('synthetic unit failure')
        kafka,stream=self.run_probe(self.state(),unavailable)
        self.assertEqual(kafka['status'],'unavailable')
        self.assertIsNone(stream['consumer_lag_messages'])
