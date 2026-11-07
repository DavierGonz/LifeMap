import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from prepare_local_env import secret_bytes


class SecretEncodingTests(unittest.TestCase):
    def test_exact_lf_without_windows_carriage_return(self):
        encoded = secret_bytes('synthetic-token-for-unit-test-0123456789')
        self.assertTrue(encoded.endswith(b'\n'))
        self.assertNotIn(b'\r', encoded)
        self.assertEqual(encoded.count(b'\n'), 1)

    def test_embedded_control_characters_are_rejected(self):
        for value in ['a' * 32 + '\r', 'a' * 32 + '\n', 'a' * 32 + ' ']:
            with self.assertRaises(ValueError):
                secret_bytes(value)
