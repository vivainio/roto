"""Check audit exit codes and baseline integrity using real pytest reports."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("audit-moto-tests.py")


class AuditTests(unittest.TestCase):
    def run_audit(self, source, baseline, update=False, regular=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tests = root / "tests/test_demo"
            tests.mkdir(parents=True)
            (tests / "test_demo.py").write_text(source)
            expected = root / "test_demo.txt"
            expected.write_text(baseline)
            result = subprocess.run(
                [sys.executable, str(SCRIPT), *(["--update"] if update else ["--run"] if regular else []),
                 str(expected)], cwd=root, capture_output=True, text=True,
            )
            return result, expected.read_text()

    def test_known_failure_is_successful_audit(self):
        result, _ = self.run_audit(
            "def test_failure(): assert False\n",
            "tests/test_demo/test_demo.py::test_failure\n",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_newly_passing_test_is_reported(self):
        baseline = "tests/test_demo/test_demo.py::test_success\n"
        result, after = self.run_audit("def test_success(): pass\n", baseline)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Newly passing tests", result.stdout)
        self.assertEqual(after, baseline)

    def test_teardown_failure_is_not_new_pass(self):
        result, _ = self.run_audit(
            "import pytest\n@pytest.fixture(autouse=True)\n"
            "def fixture():\n    yield\n    assert False\n"
            "def test_teardown(): pass\n",
            "tests/test_demo/test_demo.py::test_teardown\n",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_update_keeps_spaces_in_parameter_ids(self):
        result, after = self.run_audit(
            "import pytest\n@pytest.mark.parametrize('value', ['two words'])\n"
            "def test_failure(value): assert False\n", "# old baseline\n", update=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("::test_failure[two words]\n", after)

    def test_collection_error_preserves_baseline(self):
        baseline = "# old baseline\n"
        result, after = self.run_audit("import nonexistent_audit_module\n", baseline, update=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(after, baseline)

    def test_stale_node_is_an_error(self):
        result, _ = self.run_audit("def test_success(): pass\n", "tests/test_demo/test_demo.py::test_missing\n")
        self.assertEqual(result.returncode, 4)

    def test_regular_run_deselects_only_exact_ids(self):
        result, _ = self.run_audit(
            "def test_failure(): assert False\n"
            "def test_failure_longer_name(): assert False\n",
            "tests/test_demo/test_demo.py::test_failure\n", regular=True,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("1 failed, 1 deselected", result.stdout)


if __name__ == "__main__":
    unittest.main()
