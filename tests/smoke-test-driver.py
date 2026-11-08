#!/usr/bin/env python3
"""Exercise the Luau driver's CLI, failure isolation and case selection."""
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target/debug/roto-server"


def run(*args):
    return subprocess.run([str(BIN), "test", *map(str, args)], cwd=ROOT,
                          text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def main():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "cases.luau"
        path.write_text('''
            test.case("failing case", function()
                test.call("sqs", "CreateQueue", {QueueName = "leak"})
                test.call("s3", "CreateBucket", {Bucket = "leak-bucket"})
                test.call("s3", "PutObject", {Bucket = "leak-bucket", Key = "key", Body = "leak"})
                local machine = test.call("states", "CreateStateMachine", {
                    name = "pending", roleArn = "arn:aws:iam::123456789012:role/test",
                    definition = '{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":60,"End":true}}}',
                })
                test.call("states", "StartExecution", {stateMachineArn = machine.stateMachineArn})
                test.advance(30)
                roto.set_region("eu-west-1")
                roto.set_account("999999999999")
                error("deliberate failure")
            end)
            test.case("isolated case", function()
                test.equal(test.now(), 1735689600)
                test.equal(roto.region, "us-east-1")
                test.equal(roto.account_id, "123456789012")
                test.advance(60)
                test.equal(#test.call("states", "ListStateMachines", {}).stateMachines, 0)
                test.expect_error("AWS.SimpleQueueService.NonExistentQueue", function()
                    return test.try_call("sqs", "GetQueueUrl", {QueueName = "leak"})
                end)
                local ok = test.try_call("s3", "GetObject", {Bucket = "leak-bucket", Key = "key"})
                test.equal(ok, false)
            end)
        ''')
        result = run(directory)
        assert result.returncode != 0, result
        assert "FAIL" in result.stderr and "deliberate failure" in result.stderr, result.stderr
        assert "PASS" in result.stdout and "isolated case" in result.stdout, result.stdout
        result = run(path, "--filter", "isolated")
        assert result.returncode == 0 and "1 passed; 0 failed" in result.stdout, result
        assert "failing case" not in result.stdout + result.stderr
        result = run(path, "--filter", "not found")
        assert result.returncode != 0, result
        result = run(Path(directory) / "missing.luau")
        assert result.returncode != 0, result
        path.write_text('test.case("duplicate", function() end)\ntest.case("duplicate", function() end)')
        result = run(path)
        assert result.returncode != 0 and "duplicate case" in result.stderr, result.stderr
        path.write_text('''
            test.suite("shared suite", function(suite)
                local sequence = {}
                suite.before_all(function()
                    test.call("sqs", "CreateQueue", {QueueName = "suite-fixture"})
                    table.insert(sequence, "before_all")
                end)
                suite.before_each(function() table.insert(sequence, "before_each") end)
                suite.after_each(function() table.insert(sequence, "after_each") end)
                suite.after_all(function()
                    table.insert(sequence, "after_all")
                    test.equal(sequence, {"before_all", "before_each", "after_each", "before_each", "after_each", "after_all"})
                end)
                suite.case("fails but cleans up", function()
                    test.advance(20)
                    error("intentional suite failure")
                end)
                suite.case("shares state and continues", function()
                    assert(test.call("sqs", "GetQueueUrl", {QueueName = "suite-fixture"}).QueueUrl)
                    test.equal(test.now(), 1735689620)
                end)
            end)
            test.case("fresh standalone after suite", function()
                test.equal(test.now(), 1735689600)
                test.expect_error("AWS.SimpleQueueService.NonExistentQueue", function()
                    return test.try_call("sqs", "GetQueueUrl", {QueueName = "suite-fixture"})
                end)
            end)
        ''')
        result = run(path)
        assert result.returncode != 0, result
        assert "intentional suite failure" in result.stderr, result.stderr
        assert "shares state and continues" in result.stdout, result.stdout
        assert "fresh standalone after suite" in result.stdout, result.stdout
        assert "after_all" not in result.stderr, result.stderr
        # The suite setup must run even when only the second case is selected.
        path.write_text('''
            test.suite("filtered", function(suite)
                suite.before_all(function() test.call("sqs", "CreateQueue", {QueueName = "fixture"}) end)
                suite.case("excluded", function() error("must not run") end)
                suite.case("selected", function()
                    assert(test.call("sqs", "GetQueueUrl", {QueueName = "fixture"}).QueueUrl)
                end)
            end)
        ''')
        result = run(path, "--filter", "selected")
        assert result.returncode == 0 and "1 passed; 0 failed" in result.stdout, result.stdout + result.stderr
        assert "excluded" not in result.stdout + result.stderr
        result = run(path, "--filter", "filtered")
        assert result.returncode != 0 and "excluded" in result.stderr and "selected" in result.stdout
        path.write_text('''
            test.suite("broken fixture", function(suite)
                suite.before_all(function() error("fixture failed") end)
                suite.after_all(function() print("cleanup executed") end)
                suite.case("first", function() error("should not run") end)
                suite.case("second", function() error("should not run") end)
            end)
            test.suite("cleanup failure", function(suite)
                suite.after_all(function() error("teardown failed") end)
                suite.case("passes", function() end)
            end)
        ''')
        result = run(path)
        assert result.returncode != 0, result
        assert result.stderr.count("fixture failed") >= 2 and "should not run" not in result.stderr, result.stderr
        assert "cleanup executed" in result.stderr and "teardown failed" in result.stderr, result.stderr
        assert "cleanup failure :: passes" in result.stdout, result.stdout
        path.write_text('''
            test.suite("per-case fixture errors", function(suite)
                local counter = 0
                local cleaned = 0
                suite.before_each(function()
                    counter += 1
                    if counter == 1 then error("setup failed") end
                end)
                suite.after_each(function()
                    cleaned += 1
                    if cleaned == 1 then error("cleanup failed") end
                end)
                suite.after_each(function() print("second cleanup ran") end)
                suite.after_all(function() test.equal(cleaned, 2) end)
                suite.case("first", function() error("must not run") end)
                suite.case("second", function() test.equal(counter, 2) end)
            end)
        ''')
        result = run(path)
        assert result.returncode != 0, result
        assert "setup failed" in result.stderr and "cleanup failed" in result.stderr, result.stderr
        assert "second cleanup ran" in result.stderr and "must not run" not in result.stderr, result.stderr
        assert "per-case fixture errors :: second" in result.stdout, result.stdout
        path.write_text('test.suite("dupe", function(s) s.case("x", function() end); s.case("x", function() end) end)')
        result = run(path)
        assert result.returncode != 0 and "duplicate case" in result.stderr, result.stderr
    result = run(ROOT / "tests/luau")
    assert result.returncode == 0, result.stdout + result.stderr
    print("Luau test driver smoke checks passed")


if __name__ == "__main__":
    main()
