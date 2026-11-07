"""Pruebas minimas de cache, limpieza, relaciones y exposicion."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request
import zipfile
from datetime import datetime

from ingestion import download as source
from storage.yelp import business_document
from ingestion.scope import validate_selection
from ingestion.etl import clean_business,clean_review,families_for,area_for
from ingestion.run import connect_stage,initialize_stage,add_businesses

class Response(io.BytesIO):
    status=200
    def __init__(self,value):
        super().__init__(value); self.headers={'Content-Length':str(len(value))}

class CacheTests(unittest.TestCase):
    def test_versioned_cache_reuses_archive_and_rejects_corruption(self):
        archive=io.BytesIO()
        with zipfile.ZipFile(archive,'w') as zipped:
            for name in source.FILES.values(): zipped.writestr(name,'{}\n')
        calls=[]
        def fetch(url,credential='',offset=0):
            calls.append(url)
            if '/view/' in url:
                return Response(json.dumps({'currentVersionNumber':4}).encode())
            return Response(archive.getvalue())
        with tempfile.TemporaryDirectory() as root, patch.object(source,'fetch',fetch):
            first,transfer=source.download(root)
            second,reused=source.download(root)
            self.assertEqual(first,second)
            self.assertEqual(len(calls),2)
            self.assertTrue(reused['cache_hit'])
            self.assertEqual(reused['downloaded_bytes'],0)
            file=Path(root)/'yelp-v4'/source.FILES['review']
            file.write_text('broken',encoding='utf-8')
            with self.assertRaisesRegex(ValueError,'integrity'): source.download(root)

    def test_redirect_strips_kaggle_authorization(self):
        request=Request('https://www.kaggle.com/api/v1/datasets/download/example',headers={'Authorization':'Bearer synthetic-fixture'})
        redirect=source.SafeRedirect().redirect_request(request,None,302,'Found',{},'https://storage.googleapis.com/example')
        self.assertFalse(redirect.has_header('Authorization'))

    def test_legacy_and_token_auth(self):
        self.assertTrue(source.authorization('{"username":"fixture","key":"fixture"}').startswith('Basic '))
        self.assertEqual(source.authorization(' synthetic '),'Bearer synthetic')

class LifeTests(unittest.TestCase):
    def test_global_business_duplicates_keep_first_valid_source_row(self):
        config={'areas':[],'families':{}}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory);db=connect_stage(path/'stage.sqlite');initialize_stage(db)
            for i in range(2):
                file=path/f'part-{i}.jsonl'
                row={'business_id':'b1','latitude':i,'longitude':0,'is_open':1,'categories':[]}
                file.write_text(json.dumps(row)+'\n',encoding='utf-8')
                self.assertEqual(add_businesses(db,{'raw_ids':['b1'],'path':str(file)},config),i)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM businesses').fetchone()[0],1)
            self.assertEqual(json.loads(db.execute('SELECT doc FROM businesses').fetchone()[0])['latitude'],0)
            db.close()

    def test_coordinate_rules_and_valid_zero(self):
        row={'business_id':'b1','latitude':0,'longitude':0,'is_open':1,'categories':'Food, Restaurants, Food'}
        clean,error=clean_business(row)
        self.assertIsNone(error)
        self.assertEqual(clean['categories'],['Food','Restaurants'])
        for latitude,longitude,reason in [(None,0,'missing_coordinates'),('nan',0,'missing_coordinates'),('bad',0,'invalid_coordinates'),(float('inf'),0,'invalid_coordinates'),(91,0,'out_of_range_coordinates'),(0,-181,'out_of_range_coordinates')]:
            with self.subTest(reason=reason):
                self.assertEqual(clean_business(dict(row,latitude=latitude,longitude=longitude))[1],reason)
        self.assertEqual(clean_business(dict(row,business_id=' '))[1],'missing_business_id')

    def test_review_cleaning_and_distinct_orphan_reasons(self):
        row={'review_id':'r1','business_id':'b1','text':' good ','date':'2020-01-01 12:00:00','stars':4.0}
        self.assertEqual(clean_review(row,{'b1'},{'b1'})[0]['text'],'good')
        for change,reason in [({'review_id':''},'missing_review_id'),({'business_id':None},'missing_business_id'),({'text':'\t '},'empty_text'),({'date':'invalid'},'invalid_date'),({'date':'2020-01-01T12:00:00+00:00'},'invalid_date'),({'stars':3.5},'invalid_stars')]:
            with self.subTest(reason=reason): self.assertEqual(clean_review(dict(row,**change))[1],reason)
        self.assertEqual(clean_review(row,set(),set())[1],'orphan_business_id')
        self.assertEqual(clean_review(row,{'b1'},set())[1],'business_removed_by_cleaning')

    def test_family_union_counts_a_business_once(self):
        config={'areas':[{'id':'area','bbox':[-1,-1,1,1]}],'families':{'dining':['Food','Restaurants']}}
        row={'categories':['Food','Restaurants'],'latitude':0,'longitude':0}
        self.assertEqual(families_for(row,config),['dining'])
        self.assertEqual(area_for(row,config),'area')

    def test_geojson_life_and_censoring_follow_observed_reviews(self):
        first=datetime(2020,1,1);last=datetime(2020,1,3,12);cutoff=datetime(2020,2,1)
        row={'business_id':'b1','latitude':6.2,'longitude':-75.5,'is_open':1}
        opened=business_document(row,first,last,4,cutoff,'fixture')
        self.assertEqual(opened['location']['coordinates'],[-75.5,6.2])
        self.assertEqual(opened['business_days'],2.5)
        self.assertEqual(opened['event'],0)
        self.assertTrue(opened['censored'])
        self.assertIsNone(opened['entry_age_days'])
        self.assertEqual(opened['last_review'],last)
        row['is_open']=0
        closed=business_document(row,first,last,4,cutoff,'fixture')
        self.assertEqual(closed['event'],1)
        self.assertFalse(closed['censored'])
        self.assertEqual(business_document(row,first,first,1,cutoff,'fixture')['business_days'],0)

    def test_mass_load_requires_approved_scope_and_verified_minimum(self):
        analysis={'dataset_version':4,'fingerprint':'fixture','combinations':[{'area_id':'area','family':'family','unique_clean_reviews':1_000_000}]}
        selection={'dataset_version':4,'analysis_fingerprint':'fixture','area_id':'area','family':'family','status':'pending'}
        with self.assertRaisesRegex(ValueError,'not approved'): validate_selection(selection,analysis)
        selection['status']='approved_by_user'
        self.assertEqual(validate_selection(selection,analysis)['unique_clean_reviews'],1_000_000)
        analysis['combinations'][0]['unique_clean_reviews']=999999
        with self.assertRaisesRegex(ValueError,'million'): validate_selection(selection,analysis)

if __name__=='__main__': unittest.main()
