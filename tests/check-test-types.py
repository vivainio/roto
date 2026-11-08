#!/usr/bin/env python3
"""Check both accepted and rejected uses of the Luau test-driver definitions."""
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "target/debug/roto-server"


def check(*paths):
    return subprocess.run([str(SERVER), "--check", *map(str, paths)],
                          cwd=ROOT, capture_output=True, text=True)


def main():
    good = check(ROOT / "examples/demo/setup.luau",
                 *sorted((ROOT / "tests/luau").glob("*.luau")))
    assert good.returncode == 0, good.stdout + good.stderr
    with tempfile.TemporaryDirectory() as directory:
        bad = Path(directory) / "bad.luau"
        for code in [
            'test.case(1, function() end)',
            'test.suite("bad", function(suite) suite.before_each(1) end)',
            'test.suite(123, function(suite) end)',
            'test.advance("one")',
            'test.aws.sqs.SendMessage({QueueUrl = "url"})',
            'test.aws.states.CreateStateMachine({name = 123, roleArn = "role", definition = "{}"})',
            'local arn: number = test.aws.states.StartExecution({stateMachineArn = "arn"}).executionArn',
        ]:
            bad.write_text('--!strict\n' + code + '\n')
            result = check(bad)
            assert result.returncode != 0 and "TypeError" in result.stderr, (code, result.stdout, result.stderr)
    print("Luau test-driver type checks passed")


if __name__ == "__main__":
    main()
