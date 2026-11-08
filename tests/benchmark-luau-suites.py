#!/usr/bin/env python3
"""Compare the same five ECR cases standalone and in a suite (not a CI benchmark).

Run after building roto-server: python3 tests/benchmark-luau-suites.py [repetitions].
The suite deletes repositories after each case, adding cleanup calls. Both timings
include runner startup and discovery. No duplicate service checks are maintained here.
"""
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "target/debug/roto-server"
BUNDLE = ROOT / "services"


def main():
    repetitions = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    cases = (ROOT / "tests/luau/ecr.luau").read_text().split('\ntest.suite(', 1)[0]
    cleanup = '''suite.after_each(function()
    for _, repo in test.call("ecr", "DescribeRepositories", {}).repositories do
        test.call("ecr", "DeleteRepository", {repositoryName = repo.repositoryName})
    end
end)
'''
    with tempfile.TemporaryDirectory() as directory:
        standalone = Path(directory) / 'standalone.luau'
        suite = Path(directory) / 'suite.luau'
        standalone.write_text(cases)
        suite.write_text('test.suite("ECR", function(suite)\n' + cleanup +
                         cases.replace('test.case(', 'suite.case(') + '\nend)\n')
        timings = {"Standalone": [], "Suite": []}
        for iteration in range(repetitions):
            modes = [("Standalone", standalone), ("Suite", suite)]
            if iteration % 2:
                modes.reverse()
            for name, path in modes:
                start = time.perf_counter()
                result = subprocess.run([str(SERVER), 'test', str(path), '--lua-services', str(BUNDLE)],
                                        cwd=ROOT, check=True, capture_output=True, text=True)
                assert '5 passed; 0 failed' in result.stdout, result.stdout
                timings[name].append(time.perf_counter() - start)
    for name, times in timings.items():
        print(f"{name}: median {statistics.median(times):.3f}s; runs " + ', '.join(f"{t:.3f}s" for t in times))
    print(f"Standalone / suite median: {statistics.median(timings['Standalone']) / statistics.median(timings['Suite']):.2f}x")


if __name__ == '__main__':
    main()
