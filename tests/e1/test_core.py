"""Pruebas obligatorias E1: formula, exposicion, geohash, inferencia y entradas."""
import math
from unittest.mock import patch
import pytest
from analytics.common import closure_rate,equal
from analytics.geo import geohash,geometry
from ml.inference import murmur3,predict
from storage.yelp import main_category
from api import create_app

def test_annualized_closure_formula():
    assert closure_rate(1,365,1)==100
    assert closure_rate(2,3650)==20
    assert closure_rate(1,730,1)==50
    assert closure_rate(1,0,0) is None
    assert closure_rate(1,3649) is None
    with pytest.raises(ValueError): closure_rate(-1,365)

def test_geohash_standard_and_bounds():
    assert geohash(42.6,-5.6,5)=='ezs42'
    point=geometry(geohash(39.95,-75.16))['coordinates'][0]
    assert point[0]==point[-1]
    assert point[0][0]<=-75.16<=point[1][0]
    assert point[0][1]<=39.95<=point[2][1]

def test_stable_primary_category_does_not_expand_population():
    assert main_category(['Restaurants','Food','Pizza','Italian'])=='Italian'
    assert main_category(['Restaurants'])=='Restaurants'

def test_murmur3_standard_and_probabilities():
    assert murmur3('hello')==-488910111
    model={'num_features':8,'stopwords':[],'idf':[1]*8,'weights':[[0]*8,[0]*8,[0]*8],'intercepts':[0,0,1],'model_id':'unit-fixture'}
    result=predict('an English review',model)
    assert result['label']==2
    assert math.isclose(sum(result['probabilities'].values()),1)

def test_reduction_comparison_checks_events_and_exposure():
    assert equal({'a':(1,365.00000001)},{'a':(1,365)})
    assert not equal({'a':(0,365)},{'a':(1,365)})

@pytest.fixture
def client():
    with patch('api.repository.configure'):
        app=create_app();app.config['TESTING']=True
        yield app.test_client()

@pytest.mark.parametrize('path',['/businesses/near?lat=nan&lon=0&radius_m=10','/businesses/near?lat=0&lon=0&radius_m=10&limit=101','/businesses/near?lat=0&lat=1&lon=0&radius_m=10','/stats/closure?group_by=untrusted','/stats/closure?min_business_days=-1'])
def test_invalid_queries_are_bounded_json(client,path):
    response=client.get(path)
    assert response.status_code==400 and response.is_json

def test_self_intersecting_polygon_rejected(client):
    response=client.post('/businesses/within',json={'geometry':{'type':'Polygon','coordinates':[[[0,0],[1,1],[0,1],[1,0],[0,0]]]}})
    assert response.status_code==400

def test_empty_text_rejected(client):
    assert client.post('/predict',json={'text':'   '}).status_code==400

def test_wrong_json_shape_rejected(client):
    assert client.post('/predict',json={'text':{'$ne':None}}).status_code==400

def test_home_and_bundled_assets_are_served(client):
    response=client.get('/')
    assert response.status_code==200 and response.mimetype=='text/html'
    assert b'id="map"' in response.data and b'id="predict-form"' in response.data
    for path in ['/static/app.js','/static/app.css','/static/vendor/leaflet/leaflet.js']:
        assert client.get(path).status_code==200
