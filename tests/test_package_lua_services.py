"""Packaging contract for the optional release bundle (stdlib only)."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('package_lua_services', ROOT / 'scripts/package-lua-services.py')
package_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package_module)


class PackagingTests(unittest.TestCase):
    def test_root_manifest_is_first_and_archive_is_reproducible(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = Path(directory) / 'first.zip', Path(directory) / 'second.zip'
            package_module.package(ROOT / 'services', first)
            package_module.package(ROOT / 'services', second)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with zipfile.ZipFile(first) as archive:
                self.assertEqual(archive.namelist()[0], 'manifest.json')
                self.assertEqual(archive.getinfo('manifest.json').header_offset, 0)
                manifest = json.loads(archive.read('manifest.json'))
                self.assertEqual(len(manifest['services']), 12)
                for service in manifest['services']:
                    self.assertTrue(archive.read(service['script']))
                    if 'model' in service:
                        self.assertTrue(json.loads(archive.read(service['model']))['operations'])
                self.assertTrue(archive.read(manifest['common']))
                self.assertIn('LICENSE-MiniStack', archive.namelist())
                self.assertFalse(any(name.endswith('/manifest.json') for name in archive.namelist()))

    def test_manifest_cannot_package_files_outside_bundle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = root / 'bundle'
            bundle.mkdir()
            (root / 'outside.luau').write_text('return {}')
            manifest = {'version': 1, 'services': [{'service': 'test', 'script': '../outside.luau'}]}
            (bundle / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                package_module.package(bundle, root / 'bundle.zip')
            self.assertFalse((root / 'bundle.zip').exists())


if __name__ == '__main__':
    unittest.main()
