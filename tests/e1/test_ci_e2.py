"""Gate cerrado y transferencias reversibles: sin motor ni red."""
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import tarfile
import pytest

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('e2volumes',ROOT/'deploy/azure/e2-volumes.py')
volume=importlib.util.module_from_spec(spec);spec.loader.exec_module(volume)


@pytest.mark.parametrize('missing',['unit','models','smoke','api'])
def test_missing_gate_blocks_before_docker(tmp_path,missing):
    (tmp_path/'.ci').mkdir()
    for gate in ['unit','models','smoke','api']:
        if gate!=missing:(tmp_path/'.ci'/f'{gate}.passed').touch()
    run=subprocess.run(['bash',str(ROOT/'ci/e2_pipeline.sh'),'promote'],cwd=tmp_path,
          env={**os.environ,'BUILD_NUMBER':'1','JOB_BASE_NAME':'lifemap-gate-test','DEPLOY_TARGET':'local'},capture_output=True,text=True)
    assert run.returncode==1
    assert 'docker' not in run.stderr


def test_kafka_transfer_roundtrip_and_no_overwrite(tmp_path):
    source=tmp_path/'source';dest=tmp_path/'dest';source.mkdir();dest.mkdir()
    (source/'meta.properties').write_text('test-only-cluster')
    (source/'reviews-0').mkdir();(source/'reviews-0/log').write_bytes(b'controlled-test-bytes')
    archive=tmp_path/'kafka.tgz';meta=volume.pack(source,archive,'kafka')
    volume.restore(dest,archive,'kafka',meta['sha256'])
    assert (dest/'reviews-0/log').read_bytes()==b'controlled-test-bytes'
    with pytest.raises(AssertionError):volume.restore(dest,archive,'kafka',meta['sha256'])
    assert (dest/'meta.properties').read_text()=='test-only-cluster'


def test_checksum_or_path_attack_writes_nothing(tmp_path):
    dest=tmp_path/'dest';dest.mkdir();archive=tmp_path/'unsafe.tgz'
    with tarfile.open(archive,'w:gz') as t:
        info=tarfile.TarInfo('../escape');info.size=1;t.addfile(info,io.BytesIO(b'x'))
    with pytest.raises(AssertionError):volume.restore(dest,archive,'kafka','0'*64)
    with pytest.raises(AssertionError):volume.restore(dest,archive,'kafka',volume.sha(archive))
    assert not list(dest.iterdir()) and not (tmp_path/'escape').exists()


def test_changed_model_never_overwrites_or_partially_restores(tmp_path):
    dest=tmp_path/'dest';dest.mkdir();(dest/'risk-active.json').write_bytes(b'existing')
    archive=tmp_path/'models.tgz'
    with tarfile.open(archive,'w:gz') as t:
        for name,data in [('new-file',b'new'),('risk-active.json',b'different')]:
            info=tarfile.TarInfo(name);info.size=len(data);t.addfile(info,io.BytesIO(data))
    with pytest.raises(AssertionError):volume.restore(dest,archive,'models',volume.sha(archive))
    assert (dest/'risk-active.json').read_bytes()==b'existing' and not (dest/'new-file').exists()
