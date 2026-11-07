"""Inferencia de parametros MLlib exportados y verificados contra Spark."""
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re

def murmur3(term,seed=42):
    # MurmurHash3 x86_32 usado por FeatureHasher de Spark 3.x (Apache 2.0).
    mask=0xffffffff;data=term.encode('utf-8');h=seed
    def rot(x,n): return ((x<<n)|(x>>(32-n)))&mask
    def mix(k): return (rot((k*0xcc9e2d51)&mask,15)*0x1b873593)&mask
    end=len(data)//4*4
    for offset in range(0,end,4):
        h=rot(h^mix(int.from_bytes(data[offset:offset+4],'little')),13)
        h=(h*5+0xe6546b64)&mask
    h^=mix(int.from_bytes(data[end:],'little'))
    h^=len(data);h^=h>>16;h=h*0x85ebca6b&mask;h^=h>>13;h=h*0xc2b2ae35&mask;h^=h>>16
    return h if h<2**31 else h-2**32

def predict(text,model):
    tokens=[t for t in re.sub('[^a-z\\s]',' ',text.lower()).split() if len(t)>=2 and t not in model['stopwords']][:256]
    counts=Counter(murmur3(token)%model['num_features'] for token in tokens)
    values={key:value*model['idf'][key] for key,value in counts.items()}
    scores=[bias+sum(weights[key]*value for key,value in values.items()) for bias,weights in zip(model['intercepts'],model['weights'])]
    maximum=max(scores);exps=[math.exp(value-maximum) for value in scores];total=sum(exps)
    probabilities=[value/total for value in exps];label=max(range(3),key=probabilities.__getitem__)
    return {'label':label,'sentiment':['negative','neutral','positive'][label],'probabilities':dict(zip(['negative','neutral','positive'],probabilities)),'model_id':model['model_id']}

def load_active(root='/models'):
    active=json.loads((Path(root)/'sentiment-active.json').read_text())
    path=Path(root)/active['predictor']
    payload=path.read_bytes()
    if hashlib.sha256(payload).hexdigest()!=active['sha256']: raise ValueError('Model checksum mismatch')
    return json.loads(payload)
