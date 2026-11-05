#!/usr/bin/env python3
"""boto3 contracts for the Step Functions interpreter (`services/states`).

Needs boto3 and a built target/debug/roto-server. Starts its own ephemeral server, runs state
machines through the AWS SDK, and moves the virtual clock with POST /roto-api/clock/advance so Wait
states and execution timeouts finish without waiting.
"""
import json
import socket
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
ROLE = "arn:aws:iam::123456789012:role/r"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def code(call):
    try:
        call()
    except ClientError as e:
        return e.response["Error"]["Code"]
    raise AssertionError("expected an error")


class Server:
    def __init__(self):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc = subprocess.Popen(
            [str(ROOT / "target/debug/roto-server"), "--port", str(self.port), "--ephemeral",
             "--lua-services", str(ROOT / "services")],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), 0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise AssertionError("server did not start")
        self.sf = boto3.client("stepfunctions", endpoint_url=self.url, aws_access_key_id="test",
                               aws_secret_access_key="x", region_name="us-east-1",
                               config=Config(retries={"max_attempts": 1}))

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)

    def post(self, path, body):
        request = urllib.request.Request(self.url + path, data=json.dumps(body).encode(), method="POST")
        with urllib.request.urlopen(request) as r:
            return json.load(r)

    def get(self, path):
        with urllib.request.urlopen(self.url + path) as r:
            return json.load(r)

    def advance(self, seconds):
        result = self.post("/roto-api/clock/advance", {"seconds": seconds})
        assert result["drained"], result
        return result

    def complete(self, arn, **fields):
        request = urllib.request.Request(
            self.url + "/", data=json.dumps({"executionArn": arn, **fields}).encode(), method="POST",
            headers={"x-amz-target": "AWSStepFunctions.RotoCompleteExecution",
                     "content-type": "application/x-amz-json-1.0"})
        urllib.request.urlopen(request).read()

    def machine(self, name, definition):
        return self.sf.create_state_machine(name=name, roleArn=ROLE, definition=json.dumps(definition))["stateMachineArn"]

    def run(self, machine, payload=None, name=None):
        args = {"stateMachineArn": machine, "input": json.dumps(payload if payload is not None else {})}
        if name:
            args["name"] = name
        return self.sf.start_execution(**args)["executionArn"]

    def status(self, arn):
        return self.sf.describe_execution(executionArn=arn)

    def events(self, arn):
        return [e["type"] for e in self.sf.get_execution_history(executionArn=arn)["events"]]


def check_pass_and_paths(s):
    machine = s.machine("paths", {"StartAt": "Shape", "States": {
        "Shape": {"Type": "Pass", "Parameters": {
            "msg.$": "States.Format('hello {} you are {}', $.name, $.age)",
            "arr.$": "States.Array($.name, 1, true)",
            "len.$": "States.ArrayLength($.list)",
            "js.$": "States.JsonToString($.obj)",
            "who.$": "$$.Execution.Name",
            "machine.$": "$$.StateMachine.Name",
            "sum.$": "States.MathAdd($.age, 2)",
            "parsed.$": "States.StringToJson('{\"a\": 1}')",
            "first.$": "$.list[0]",
            "all.$": "$.items[*].id",
            "fixed": {"k": "v"},
        }, "ResultPath": "$.shaped", "Next": "Keep"},
        "Keep": {"Type": "Pass", "InputPath": "$.shaped", "Result": {"ignored": True},
                 "ResultPath": "$.extra.deep", "OutputPath": "$", "End": True},
    }})
    arn = s.run(machine, {"name": "Ada", "age": 36, "list": [7, 8, 9], "obj": {"x": 1},
                          "items": [{"id": "a"}, {"id": "b"}]}, name="shape")
    done = s.status(arn)
    assert done["status"] == "SUCCEEDED" and "stopDate" in done, done
    out = json.loads(done["output"])
    assert out["extra"]["deep"] == {"ignored": True}, out
    p = out["shaped"]
    shaped = p["msg"], p["arr"], p["len"], p["js"], p["who"], p["machine"], p["sum"], p["parsed"], \
        p["first"], p["all"], p["fixed"]
    assert shaped == ("hello Ada you are 36", ["Ada", 1, True], 3, '{"x":1}', "shape", "paths", 38, {"a": 1}, 7,
                      ["a", "b"], {"k": "v"}), shaped
    assert json.loads(done["input"])["name"] == "Ada"
    assert s.events(arn) == ["ExecutionStarted", "PassStateEntered", "PassStateExited", "PassStateEntered",
                             "PassStateExited", "ExecutionSucceeded"]

    # ResultPath null keeps the input; OutputPath narrows; a missing path is a runtime failure.
    machine = s.machine("discard", {"StartAt": "A", "States": {
        "A": {"Type": "Pass", "Result": 5, "ResultPath": None, "Next": "B"},
        "B": {"Type": "Pass", "OutputPath": "$.keep", "End": True}}})
    assert json.loads(s.status(s.run(machine, {"keep": {"n": 1}}))["output"]) == {"n": 1}
    machine = s.machine("missing", {"StartAt": "A", "States": {
        "A": {"Type": "Pass", "InputPath": "$.nope", "End": True}}})
    failed = s.status(s.run(machine, {}))
    assert (failed["status"], failed["error"]) == ("FAILED", "States.Runtime"), failed


def check_choice_and_fail(s):
    machine = s.machine("route", {"StartAt": "Pick", "States": {
        "Pick": {"Type": "Choice", "Choices": [
            {"Variable": "$.n", "NumericGreaterThan": 10, "Next": "Big"},
            {"And": [{"Variable": "$.s", "StringMatches": "log-*"}, {"Not": {"Variable": "$.n", "NumericEquals": 0}}],
             "Next": "Log"},
            {"Variable": "$.when", "TimestampLessThan": "2030-01-01T00:00:00Z", "Next": "Early"},
            {"Variable": "$.flag", "BooleanEquals": True, "Next": "Flagged"},
            {"Variable": "$.opt", "IsPresent": False, "Next": "NoOpt"}],
         "Default": "Other"},
        "Big": {"Type": "Pass", "Result": "big", "End": True},
        "Log": {"Type": "Pass", "Result": "log", "End": True},
        "Early": {"Type": "Pass", "Result": "early", "End": True},
        "Flagged": {"Type": "Pass", "Result": "flagged", "End": True},
        "NoOpt": {"Type": "Pass", "Result": "noopt", "End": True},
        "Other": {"Type": "Fail", "Error": "Nope", "Cause": "no rule matched"}}})
    cases = [
        ({"n": 11}, "big"), ({"n": 1, "s": "log-1"}, "log"), ({"n": 1, "s": "x", "when": "2029-06-01T10:00:00+02:00"}, "early"),
        ({"n": 1, "s": "x", "when": "2031-01-01T00:00:00Z", "flag": True}, "flagged"),
        ({"n": 1, "s": "x", "when": "2031-01-01T00:00:00Z", "flag": False}, "noopt"),
        ({"n": 1, "s": "x", "when": "2031-01-01T00:00:00Z", "flag": False, "opt": 1}, None)]
    for payload, expected in cases:
        done = s.status(s.run(machine, payload))
        if expected is None:
            assert (done["status"], done["error"], done["cause"]) == ("FAILED", "Nope", "no rule matched"), done
        else:
            assert (done["status"], json.loads(done["output"])) == ("SUCCEEDED", expected), (payload, done)
    arn = s.run(machine, {"n": 1, "s": "x", "when": "2031-01-01T00:00:00Z", "flag": False, "opt": 1})
    assert s.events(arn)[-3:] == ["ChoiceStateExited", "FailStateEntered", "ExecutionFailed"]
    # No matching rule and no Default.
    machine = s.machine("nodefault", {"StartAt": "Pick", "States": {
        "Pick": {"Type": "Choice", "Choices": [{"Variable": "$.n", "NumericEquals": 1, "Next": "Ok"}]},
        "Ok": {"Type": "Succeed"}}})
    assert s.status(s.run(machine, {"n": 2}))["error"] == "States.NoChoiceMatched"
    assert s.status(s.run(machine, {"n": 1}))["status"] == "SUCCEEDED"


def check_wait_and_clock(s):
    machine = s.machine("waits", {"StartAt": "Pause", "States": {
        "Pause": {"Type": "Wait", "Seconds": 3600, "Next": "Then"},
        "Then": {"Type": "Wait", "SecondsPath": "$.more", "Next": "Done"},
        "Done": {"Type": "Pass", "Result": "finished", "End": True}}})
    arn = s.run(machine, {"more": 600})
    assert s.status(arn)["status"] == "RUNNING"
    assert s.events(arn) == ["ExecutionStarted", "WaitStateEntered"]
    s.advance(3599)
    assert s.status(arn)["status"] == "RUNNING"
    s.advance(2)  # the first wait ends and the second begins
    assert s.status(arn)["status"] == "RUNNING"
    assert s.events(arn)[-2:] == ["WaitStateExited", "WaitStateEntered"]
    # One long advance steps through the remaining timer.
    s.advance(86400)
    done = s.status(arn)
    assert (done["status"], json.loads(done["output"])) == ("SUCCEEDED", "finished"), done
    assert s.events(arn) == ["ExecutionStarted", "WaitStateEntered", "WaitStateExited", "WaitStateEntered",
                             "WaitStateExited", "PassStateEntered", "PassStateExited", "ExecutionSucceeded"]
    stamps = [e["timestamp"].timestamp() for e in s.sf.get_execution_history(executionArn=arn)["events"]]
    assert stamps[2] - stamps[1] >= 3600 and stamps[4] - stamps[3] >= 600, stamps

    # An absolute timestamp wait, and one already in the past.
    now = datetime.fromtimestamp(s.get("/roto-api/clock")["now"], timezone.utc)
    machine = s.machine("until", {"StartAt": "Until", "States": {
        "Until": {"Type": "Wait", "TimestampPath": "$.at", "End": True}}})
    later = s.run(machine, {"at": (now + timedelta(seconds=120)).strftime("%Y-%m-%dT%H:%M:%SZ")})
    past = s.run(machine, {"at": (now - timedelta(seconds=120)).strftime("%Y-%m-%dT%H:%M:%SZ")})
    assert s.status(past)["status"] == "SUCCEEDED" and s.status(later)["status"] == "RUNNING"
    s.advance(121)
    assert s.status(later)["status"] == "SUCCEEDED"

    # TimeoutSeconds on the machine, and Stop cancelling a waiting execution.
    machine = s.machine("slow", {"StartAt": "Hang", "TimeoutSeconds": 60, "States": {
        "Hang": {"Type": "Wait", "Seconds": 600, "End": True}}})
    timed, stopped = s.run(machine), s.run(machine)
    s.sf.stop_execution(executionArn=stopped, error="Halt", cause="by hand")
    s.advance(61)
    done = s.status(timed)
    assert (done["status"], done["error"]) == ("TIMED_OUT", "States.Timeout"), done
    assert s.events(timed)[-1] == "ExecutionTimedOut"
    s.advance(1000)
    assert s.status(stopped)["status"] == "ABORTED" and s.events(stopped)[-1] == "ExecutionAborted"
    assert s.status(stopped)["cause"] == "by hand"


def check_tasks_stay_running(s):
    machine = s.machine("tasky", {"StartAt": "Prep", "States": {
        "Prep": {"Type": "Pass", "Result": {"ready": True}, "Next": "Work"},
        "Work": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "End": True}}})
    arn = s.run(machine)
    assert s.status(arn)["status"] == "RUNNING"
    assert s.events(arn) == ["ExecutionStarted", "PassStateEntered", "PassStateExited"]
    s.complete(arn, status="SUCCEEDED", output='{"by":"fixture"}')
    done = s.status(arn)
    assert (done["status"], done["output"]) == ("SUCCEEDED", '{"by":"fixture"}'), done
    assert s.events(arn)[-1] == "ExecutionSucceeded"


def check_history_pages_and_validation(s):
    machine = s.machine("pages", {"StartAt": "A", "States": {
        "A": {"Type": "Pass", "Next": "B"}, "B": {"Type": "Pass", "Next": "C"}, "C": {"Type": "Succeed"}}})
    arn = s.run(machine)
    first = s.sf.get_execution_history(executionArn=arn, maxResults=3)
    assert len(first["events"]) == 3 and "nextToken" in first
    rest = s.sf.get_execution_history(executionArn=arn, maxResults=100, nextToken=first["nextToken"])
    ids = [e["id"] for e in first["events"] + rest["events"]]
    assert ids == list(range(1, len(ids) + 1)) and "nextToken" not in rest
    reverse = s.sf.get_execution_history(executionArn=arn, reverseOrder=True)["events"]
    assert [e["id"] for e in reverse] == ids[::-1]
    assert [e["previousEventId"] for e in reverse][-1] == 0
    assert code(lambda: s.sf.get_execution_history(executionArn=arn + "x")) == "ExecutionDoesNotExist"
    for bad in ({"StartAt": "Z", "States": {"A": {"Type": "Succeed"}}},
                {"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": "Missing"}}},
                {"StartAt": "A", "States": {"A": {"Type": "Pass"}}},
                {"StartAt": "A", "States": {"A": {"Type": "Nope"}}},
                {"StartAt": "A", "States": {"A": {"Type": "Wait", "End": True}}},
                {"States": {}}):
        assert code(lambda: s.machine("bad", bad)) == "InvalidDefinition", bad
    # Starting twice with the same name and input is idempotent while running.
    waiting = s.machine("idem", {"StartAt": "W", "States": {"W": {"Type": "Wait", "Seconds": 5, "End": True}}})
    one = s.run(waiting, {"a": 1}, name="same")
    assert s.run(waiting, {"a": 1}, name="same") == one
    assert code(lambda: s.run(waiting, {"a": 2}, name="same")) == "ExecutionAlreadyExists"


def check_loops_fail_instead_of_hanging(s):
    machine = s.machine("loop", {"StartAt": "A", "States": {
        "A": {"Type": "Pass", "Next": "B"}, "B": {"Type": "Pass", "Next": "A"}}})
    started = time.monotonic()
    arn = s.run(machine)
    s.advance(0)
    done = s.status(arn)
    assert (done["status"], done["error"]) == ("FAILED", "States.Runtime"), done
    assert time.monotonic() - started < 60


def main():
    server = Server()
    try:
        for check in (check_pass_and_paths, check_choice_and_fail, check_wait_and_clock,
                      check_tasks_stay_running, check_history_pages_and_validation,
                      check_loops_fail_instead_of_hanging):
            started = time.monotonic()
            check(server)
            print(f"{check.__name__}: ok ({time.monotonic() - started:.1f}s)")
        unsupported = json.dumps(server.get("/roto-api/unsupported"))
        assert "GetExecutionHistory" not in unsupported, unsupported
    finally:
        server.stop()


if __name__ == "__main__":
    main()
