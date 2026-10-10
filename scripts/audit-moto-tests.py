#!/usr/bin/env python3
"""Retry known failures without changing the baseline; fail on newly passing tests."""

import sys
from pathlib import Path
import pytest


class Audit:
    def __init__(self, excluded=()):
        self.passed = set()
        self.failed = set()
        self.excluded = set(excluded)
        self.active_test = None
        self.original_send = None

    def pytest_configure(self, config):
        # Add a harmless correlation header after signing so the server trace can
        # associate each HTTP request with the Moto test that made it.
        from botocore.endpoint import Endpoint

        self.original_send = Endpoint._send
        original_send = self.original_send
        audit = self

        def traced_send(endpoint, request):
            if audit.active_test:
                trace_id = "".join(
                    char if char.isascii() and char.isprintable() else "_"
                    for char in audit.active_test
                )
                request.headers["X-Roto-Trace-ID"] = trace_id[:256]
            return original_send(endpoint, request)

        Endpoint._send = traced_send

    def pytest_unconfigure(self, config):
        if self.original_send is not None:
            from botocore.endpoint import Endpoint

            Endpoint._send = self.original_send

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item, nextitem):
        self.active_test = item.nodeid
        yield
        self.active_test = None

    def pytest_collection_modifyitems(self, session, config, items):
        if self.excluded:
            deselected = [item for item in items if item.nodeid in self.excluded]
            items[:] = [item for item in items if item.nodeid not in self.excluded]
            config.hook.pytest_deselected(items=deselected)

    def pytest_runtest_logreport(self, report):
        if report.when == "call" and report.passed:
            self.passed.add(report.nodeid)
        elif report.failed:
            self.passed.discard(report.nodeid)
            self.failed.add(report.nodeid)


def main():
    update = sys.argv[1] == "--update"
    regular = sys.argv[1] == "--run"
    offset = 2 if update or regular else 1
    baseline = Path(sys.argv[offset])
    nodes = [
        line.strip()
        for line in baseline.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ] if baseline.exists() else []
    if not nodes and not update and not regular:
        print("No known failures to audit.")
        return 0
    audit = Audit(nodes if regular else ())
    result = pytest.main(
        [*(["tests/" + baseline.stem] if update or regular else nodes),
         "-q", "-p", "no:cacheprovider", *([] if regular else ["--tb=no"]),
         *sys.argv[offset + 1:]],
        plugins=[audit],
    )
    if regular:
        return int(result)
    if result not in (pytest.ExitCode.OK, pytest.ExitCode.TESTS_FAILED):
        return int(result)
    if update:
        baseline.parent.mkdir(parents=True, exist_ok=True)
        baseline.write_text(
            f"# Known failures for {baseline.stem} (regenerate: UPDATE_EXPECTED=1 "
            f"scripts/run-moto-tests.sh {baseline.stem})\n"
            + "".join(node + "\n" for node in sorted(audit.failed))
        )
        print("Wrote " + str(baseline))
        return 0
    if audit.passed:
        print("\nNewly passing tests: remove these from " + str(baseline))
        for node in sorted(audit.passed):
            print(node)
        return 1
    print("\nNo newly passing known failures.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
