"""Guardas de aislamiento y salud, sin servicios ni paquetes de terceros."""
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from validate_infrastructure import load_model, validate_model, validate_sources

spec = importlib.util.spec_from_file_location("lifemap_probes", ROOT / "api" / "probes.py")
probes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probes)


class InfrastructureIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = load_model()

    def test_real_configuration_and_build_inputs(self):
        self.assertEqual(validate_model(self.model), [])
        self.assertEqual(validate_sources(), [])

    def test_rejects_public_admin_port(self):
        model = copy.deepcopy(self.model)
        model["services"]["jenkins"]["ports"][0]["host_ip"] = "0.0.0.0"
        self.assertTrue(any("non-loopback" in error for error in validate_model(model)))

    def test_dask_coordinator_and_workers_share_persistent_cache(self):
        for name in ('dask-scheduler','dask-worker-1','dask-worker-2'):
            mounts=self.model['services'][name]['volumes']
            self.assertTrue(any(m['type']=='volume' and m['source']=='dataset_cache' and m['target']=='/data' for m in mounts),name)

    def test_rejects_foreign_volume(self):
        model = copy.deepcopy(self.model)
        model["volumes"]["mongo_data"]["name"] = "parcialbigdata_mongo_data"
        self.assertTrue(any("not scoped" in error for error in validate_model(model)))

    def test_rejects_protected_secret_path(self):
        model = copy.deepcopy(self.model)
        model["secrets"]["mongo_root_password"]["file"] = "D:/Proyects/ParcialBigData/secrets/forbidden"
        self.assertTrue(any("inside LifeMap" in error for error in validate_model(model)))

    def test_rejects_admin_secret_in_api(self):
        model = copy.deepcopy(self.model)
        model["services"]["api"]["secrets"].append({"source": "mongo_root_password"})
        self.assertTrue(any("administrator" in error for error in validate_model(model)))

    def test_rejects_mount_from_discos(self):
        model = copy.deepcopy(self.model)
        model["services"]["spark-worker"]["volumes"].append({
            "type": "bind", "source": "D:/Proyects/Python/BigData/Discos/data",
            "target": "/foreign-data",
        })
        self.assertTrue(any("unexpected host bind" in error for error in validate_model(model)))


class DependencyHealthTests(unittest.TestCase):
    def test_spark_without_worker_is_not_ready(self):
        response = io.BytesIO(json.dumps({"status": "ALIVE", "workers": []}).encode())
        with patch.dict(os.environ, {"SPARK_STATUS_URL": "http://unused"}), patch.object(probes, "urlopen", return_value=response):
            self.assertEqual(probes.spark_probe()["status"], "unavailable")

    def test_dask_requires_two_workers(self):
        response = io.BytesIO(b'{"workers": 1}')
        with patch.dict(os.environ, {"DASK_STATUS_URL": "http://unused"}), patch.object(probes, "urlopen", return_value=response):
            self.assertEqual(probes.dask_probe()["status"], "unavailable")

    def test_dependency_failure_is_sanitized(self):
        with patch.dict(os.environ, {"SPARK_STATUS_URL": "http://unused"}), patch.object(probes, "urlopen", side_effect=RuntimeError("private-diagnostic")):
            self.assertEqual(probes.spark_probe(), {"status": "unavailable"})


if __name__ == "__main__":
    unittest.main()
