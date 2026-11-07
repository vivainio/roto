"""Fixture contract against AWS's runner; does not assert Roto compatibility."""

import json

from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from handler import handler


def test_wait_replays_without_repeating_completed_steps(tmp_path):
    audit_path = tmp_path / "calls.jsonl"
    with DurableFunctionTestRunner(handler, skip_time=True) as runner:
        result = runner.run(
            input=json.dumps({"value": 7, "audit_path": str(audit_path)}),
            execution_timeout=30,
        )

    assert result.status.value == "SUCCEEDED", result.error
    assert json.loads(result.result) == {"value": 28}
    assert [operation.name for operation in result.operations] == [
        "first", "pause", "second"
    ]
    assert all(operation.status.value == "SUCCEEDED" for operation in result.operations)
    calls = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert calls.count("handler") >= 2, calls
    assert calls.count("first") == 1, calls
    assert calls.count("second") == 1, calls
