#!/usr/bin/env python3
"""boto3 contracts for the Step Functions interpreter (`services/states`) and the virtual clock.

Needs boto3 and a built target/debug/roto-server. Starts its own ephemeral server, runs state
machines through the AWS SDK, and moves the virtual clock with POST /roto-api/clock/advance so Wait
states and execution timeouts finish without waiting.
"""
import json
from concurrent.futures import ThreadPoolExecutor
import socket
import subprocess
import tempfile
import time
import urllib.error
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


# Lambda functions with command executors, and a queue, for the Task state checks.
SETUP = """
roto.lambda.fn("echo", { timeout = 5, executor = { command = { "sh", "-c", "cat" } } })
roto.lambda.fn("boom", { timeout = 5, executor = { command = { "sh", "-c", "echo oops >&2; exit 1" } } })
roto.lambda.fn("slow", { timeout = 5, executor = { command = { "sh", "-c", "sleep 2; cat" } } })
roto.aws.sqs.CreateQueue({ QueueName = "jobs" })
"""


class Server:
    def __init__(self):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.setup = tempfile.NamedTemporaryFile("w", suffix=".luau", delete=False)
        self.setup.write(SETUP)
        self.setup.close()
        self.proc = subprocess.Popen(
            [str(ROOT / "target/debug/roto-server"), "--port", str(self.port), "--ephemeral",
             "--lua-services", str(ROOT / "services"), "--setup", self.setup.name],
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
        self.sqs = boto3.client("sqs", endpoint_url=self.url, aws_access_key_id="test",
                                aws_secret_access_key="x", region_name="us-east-1")
        self.sf = boto3.client("stepfunctions", endpoint_url=self.url, aws_access_key_id="test",
                               aws_secret_access_key="x", region_name="us-east-1",
                               config=Config(retries={"max_attempts": 1}, inject_host_prefix=False))

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)
        Path(self.setup.name).unlink(missing_ok=True)

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


def check_parallel_and_map(s):
    machine = s.machine("fanout", {"StartAt": "Both", "States": {
        "Both": {"Type": "Parallel", "ResultPath": "$.both", "Next": "Each", "Branches": [
            {"StartAt": "A", "States": {"A": {"Type": "Pass", "Result": "a", "End": True}}},
            {"StartAt": "Echo", "States": {"Echo": {
                "Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:echo",
                "Parameters": {"seen.$": "$.items"}, "End": True}}}]},
        "Each": {"Type": "Map", "ItemsPath": "$.items", "ResultPath": "$.doubled",
                 "ItemSelector": {"v.$": "$$.Map.Item.Value", "i.$": "$$.Map.Item.Index", "k.$": "$.k"},
                 "ItemProcessor": {"StartAt": "Pick", "States": {
                     "Pick": {"Type": "Choice", "Choices": [
                         {"Variable": "$.v", "NumericGreaterThan": 1, "Next": "Big"}], "Default": "Small"},
                     "Big": {"Type": "Pass", "Parameters": {"r.$": "States.MathAdd($.v, $.v)", "i.$": "$.i"}, "End": True},
                     "Small": {"Type": "Pass", "Parameters": {"r.$": "$.v", "i.$": "$.i"}, "End": True}}},
                 "End": True}}})
    arn = s.run(machine, {"items": [1, 2, 3], "k": "x"})
    done = s.status(arn)
    assert done["status"] == "SUCCEEDED", done
    out = json.loads(done["output"])
    assert out["both"] == ["a", {"seen": [1, 2, 3]}], out
    assert out["doubled"] == [{"r": 1, "i": 0}, {"r": 4, "i": 1}, {"r": 6, "i": 2}], out

    # A failing branch fails the state; Catch and Retry apply to it. A Retry inside a branch waits
    # on a timer like any other, without holding up its sibling.
    machine = s.machine("fanout-fail", {"StartAt": "Both", "States": {
        "Both": {"Type": "Parallel", "Catch": [{"ErrorEquals": ["Oops"], "ResultPath": "$.err", "Next": "Ok"}],
                 "Branches": [
                     {"StartAt": "Fine", "States": {"Fine": {"Type": "Pass", "End": True}}},
                     {"StartAt": "Boom", "States": {"Boom": {
                         "Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:boom",
                         "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2}],
                         "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Bad"}], "Next": "Fine2"},
                         "Fine2": {"Type": "Pass", "End": True},
                         "Bad": {"Type": "Fail", "Error": "Oops", "Cause": "branch failed"}}}],
                 "End": True},
        "Ok": {"Type": "Succeed"}}})
    arn = s.run(machine, {})
    assert s.status(arn)["status"] == "RUNNING"  # the branch is backing off before its retry
    s.advance(10)
    done = s.status(arn)
    assert done["status"] == "SUCCEEDED" and json.loads(done["output"])["err"]["Error"] == "Oops", done


def check_branches_run_concurrently(s):
    queue_url = s.sqs.get_queue_url(QueueName="jobs")["QueueUrl"]
    s.sqs.purge_queue(QueueUrl=queue_url)
    # Branch A waits 60s then answers; branch B is a callback that needs a token.
    machine = s.machine("concurrent", {"StartAt": "Both", "States": {
        "Both": {"Type": "Parallel", "End": True, "Branches": [
            {"StartAt": "Sleep", "States": {
                "Sleep": {"Type": "Wait", "Seconds": 60, "Next": "A"},
                "A": {"Type": "Pass", "Result": "slept", "End": True}}},
            {"StartAt": "Ask", "States": {"Ask": {
                "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
                "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "$$.Task.Token"}, "End": True}}}]}}})
    arn = s.run(machine, {})
    token = s.sqs.receive_message(QueueUrl=queue_url)["Messages"][0]["Body"]  # B started while A waits
    s.advance(60)
    assert s.status(arn)["status"] == "RUNNING"  # A is done; B still needs its token
    s.sf.send_task_success(taskToken=token, output='"asked"')
    done = s.status(arn)
    assert (done["status"], json.loads(done["output"])) == ("SUCCEEDED", ["slept", "asked"]), done
    history = s.sf.get_execution_history(executionArn=arn)["events"]
    events = {event["type"]: event for event in history}
    assert events["WaitStateEntered"]["previousEventId"] == events["ParallelStateStarted"]["id"]
    assert events["TaskStateEntered"]["previousEventId"] == events["ParallelStateStarted"]["id"]
    assert events["WaitStateExited"]["previousEventId"] == events["WaitStateEntered"]["id"]
    assert events["PassStateEntered"]["previousEventId"] == events["WaitStateExited"]["id"]
    assert events["TaskSucceeded"]["previousEventId"] == events["TaskStarted"]["id"]

    # A Map whose items each wait for a token; the results keep the item order whichever finishes first.
    s.sqs.purge_queue(QueueUrl=queue_url)
    machine = s.machine("map-callbacks", {"StartAt": "Each", "States": {"Each": {
        "Type": "Map", "ItemsPath": "$.items", "End": True,
        "ItemSelector": {"item.$": "$$.Map.Item.Value"},
        "ItemProcessor": {"StartAt": "Ask", "States": {"Ask": {
            "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "States.JsonToString($)",
                           "MessageAttributes": {"token": {"DataType": "String", "StringValue.$": "$$.Task.Token"}}},
            "ResultSelector": {"answer.$": "$"}, "End": True}}}}}})
    arn = s.run(machine, {"items": ["x", "y", "z"]})
    tokens = {}
    for _ in range(3):
        for m in s.sqs.receive_message(QueueUrl=queue_url, MessageAttributeNames=["All"],
                                       MaxNumberOfMessages=10).get("Messages", []):
            tokens[json.loads(m["Body"])["item"]] = m["MessageAttributes"]["token"]["StringValue"]
    assert sorted(tokens) == ["x", "y", "z"], tokens
    for item in ("z", "x", "y"):
        assert s.status(arn)["status"] == "RUNNING"
        s.sf.send_task_success(taskToken=tokens[item], output=json.dumps(item.upper()))
    done = s.status(arn)
    assert json.loads(done["output"]) == [{"answer": "X"}, {"answer": "Y"}, {"answer": "Z"}], done
    history = s.sf.get_execution_history(executionArn=arn)["events"]
    map_started = next(event for event in history if event["type"] == "MapStateStarted")
    iterations = {event["mapIterationStartedEventDetails"]["index"]: event for event in history
                  if event["type"] == "MapIterationStarted"}
    assert all(event["previousEventId"] == map_started["id"] for event in iterations.values())
    for event in history:
        if event["type"] == "TaskStateEntered":
            item = json.loads(event["stateEnteredEventDetails"]["input"])["item"]
            assert event["previousEventId"] == iterations[["x", "y", "z"].index(item)]["id"]

    # One branch failing aborts its siblings: their tokens stop working and their timers are gone.
    s.sqs.purge_queue(QueueUrl=queue_url)
    machine = s.machine("abort-siblings", {"StartAt": "Both", "States": {"Both": {
        "Type": "Parallel", "End": True, "Branches": [
            {"StartAt": "Ask", "States": {"Ask": {
                "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
                "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "$$.Task.Token"}, "End": True}}},
            {"StartAt": "Nope", "States": {"Nope": {"Type": "Fail", "Error": "Nope", "Cause": "x"}}}]}}})
    arn = s.run(machine, {})
    failed = s.status(arn)
    assert (failed["status"], failed["error"]) == ("FAILED", "Nope"), failed
    history = s.sf.get_execution_history(executionArn=arn)["events"]
    by_id = {event["id"]: event for event in history}
    current = next(event for event in history if event["type"] == "ExecutionFailed")
    predecessors = []
    while current["previousEventId"]:
        current = by_id[current["previousEventId"]]
        predecessors.append(current["type"])
    assert "ParallelStateFailed" in predecessors and "ParallelStateEntered" in predecessors, predecessors
    orphan = s.sqs.receive_message(QueueUrl=queue_url)["Messages"][0]["Body"]
    assert code(lambda: s.sf.send_task_success(taskToken=orphan, output="{}")) == "TaskTimedOut"
    s.advance(10_000)  # nothing left to fire
    assert s.status(arn)["status"] == "FAILED"


def check_map_max_concurrency_and_distributed(s):
    queue_url = s.sqs.get_queue_url(QueueName="jobs")["QueueUrl"]
    s.sqs.purge_queue(QueueUrl=queue_url)
    machine = s.machine("limited", {"StartAt": "Each", "States": {"Each": {
        "Type": "Map", "ItemsPath": "$.items", "MaxConcurrency": 2, "End": True,
        "ItemProcessor": {"StartAt": "Ask", "States": {"Ask": {
            "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "$$.Task.Token"},
            "ResultSelector": {"got.$": "$"}, "End": True}}}}}})
    arn = s.run(machine, {"items": [1, 2, 3, 4, 5]})

    def tokens():
        out = []
        while True:
            batch = s.sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get("Messages", [])
            if not batch:
                return out
            for m in batch:
                s.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                out.append(m["Body"])

    answered = 0
    while answered < 5:
        batch = tokens()
        assert 0 < len(batch) <= 2, (answered, len(batch))  # never more than two at once
        for token in batch:
            answered += 1
            s.sf.send_task_success(taskToken=token, output=str(answered))
    done = s.status(arn)
    assert json.loads(done["output"]) == [{"got": n} for n in (1, 2, 3, 4, 5)], done

    distributed = {"StartAt": "Each", "States": {"Each": {
        "Type": "Map", "ItemReader": {"Resource": "arn:aws:states:::s3:listObjectsV2"}, "End": True,
        "ItemProcessor": {"ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
                          "StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}}}}
    assert code(lambda: s.machine("distributed", distributed)) == "InvalidDefinition"
    assert code(lambda: s.machine("bad-concurrency", {"StartAt": "Each", "States": {"Each": {
        "Type": "Map", "MaxConcurrency": -1, "End": True,
        "ItemProcessor": {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}}}})) == "InvalidDefinition"


def check_nested_executions(s):
    quick = s.machine("child-quick", {"StartAt": "P", "States": {
        "P": {"Type": "Pass", "Parameters": {"doubled.$": "States.MathAdd($.n, $.n)"}, "End": True}}})
    slow = s.machine("child-slow", {"StartAt": "W", "States": {
        "W": {"Type": "Wait", "Seconds": 30, "Next": "P"},
        "P": {"Type": "Pass", "Parameters": {"slept.$": "$.n"}, "End": True}}})
    bad = s.machine("child-bad", {"StartAt": "F", "States": {"F": {"Type": "Fail", "Error": "Kaput", "Cause": "no"}}})

    def parent(child, resource):
        return s.machine("parent-" + resource.replace(":", "-").replace(".", "-") + child.rsplit(":", 1)[1], {
            "StartAt": "Call", "States": {
                "Call": {"Type": "Task", "Resource": resource,
                         "Parameters": {"StateMachineArn": child, "Input": {"n.$": "$.n"}},
                         "ResultSelector": {"out.$": "$.Output", "status.$": "$.Status"}, "ResultPath": "$.child",
                         "Catch": [{"ErrorEquals": ["States.TaskFailed"], "ResultPath": "$.err", "Next": "Done"}],
                         "Next": "Done"},
                "Done": {"Type": "Succeed"}}})

    sync = "arn:aws:states:::states:startExecution.sync:2"
    quick_arn = s.run(parent(quick, sync), {"n": 4})
    s.advance(0)
    out = json.loads(s.status(quick_arn)["output"])
    assert out["child"] == {"out": {"doubled": 8}, "status": "SUCCEEDED"}, out  # finished inside the call

    slow_parent = parent(slow, sync)
    arn = s.run(slow_parent, {"n": 7})
    s.advance(0)
    assert s.status(arn)["status"] == "RUNNING"  # waiting on the child's Wait
    s.advance(30)
    out = json.loads(s.status(arn)["output"])
    assert out["child"] == {"out": {"slept": 7}, "status": "SUCCEEDED"}, out

    bad_arn = s.run(parent(bad, sync), {"n": 1})
    s.advance(0)
    out = json.loads(s.status(bad_arn)["output"])
    assert json.loads(out["err"]["Cause"])["Status"] == "FAILED", out

    # Stopping the parent stops the child it is waiting for.
    arn = s.run(slow_parent, {"n": 2})
    s.advance(0)
    child = s.sf.list_executions(stateMachineArn=slow, statusFilter="RUNNING")["executions"]
    assert len(child) == 1, child  # the first child finished when the clock advanced
    s.sf.stop_execution(executionArn=arn)
    assert s.status(child[0]["executionArn"])["status"] == "ABORTED"
    s.advance(60)  # the child's Wait timer is gone

    # A branch dropped because its sibling failed stops the child it was waiting for as well.
    running_before = len(s.sf.list_executions(stateMachineArn=slow, statusFilter="RUNNING")["executions"])
    both = s.machine("branch-child", {"StartAt": "Both", "States": {"Both": {
        "Type": "Parallel", "End": True, "Branches": [
            {"StartAt": "Call", "States": {"Call": {
                "Type": "Task", "Resource": sync, "End": True,
                "Parameters": {"StateMachineArn": slow, "Input": {"n": 3}}}}},
            {"StartAt": "Nope", "States": {"Nope": {"Type": "Wait", "Seconds": 1, "Next": "Boom"},
                                           "Boom": {"Type": "Fail", "Error": "Boom", "Cause": "x"}}}]}}})
    arn = s.run(both, {})
    s.advance(0)
    assert s.status(arn)["status"] == "RUNNING"
    started = s.sf.list_executions(stateMachineArn=slow, statusFilter="RUNNING")["executions"]
    assert len(started) == running_before + 1, started
    s.advance(2)  # the sibling fails; the branch waiting on the child is dropped
    assert s.status(arn)["status"] == "FAILED"
    assert len(s.sf.list_executions(stateMachineArn=slow, statusFilter="RUNNING")["executions"]) == running_before

    # Request Response starts the child and moves on without waiting for it.
    started = s.status(s.run(s.machine("fire-and-forget", {"StartAt": "Call", "States": {"Call": {
        "Type": "Task", "Resource": "arn:aws:states:::states:startExecution",
        "Parameters": {"StateMachineArn": slow, "Input": {"n": 1}}, "ResultPath": "$.child", "End": True}}}), {}))
    assert started["status"] == "SUCCEEDED" and "ExecutionArn" in json.loads(started["output"])["child"], started
    blocking_child = s.machine("child-slow-lambda", {"StartAt": "Call", "States": {
        "Call": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:slow", "End": True}}})
    fire = s.machine("fire-child-slow-lambda", {"StartAt": "Call", "States": {"Call": {
        "Type": "Task", "Resource": "arn:aws:states:::states:startExecution", "TimeoutSeconds": 1,
        "Parameters": {"StateMachineArn": blocking_child}, "End": True}}})
    start = time.monotonic()
    parent_arn = s.run(fire, {})
    assert time.monotonic() - start < 1.5
    completed_parent = s.status(parent_arn)
    assert completed_parent["status"] == "SUCCEEDED"
    child_arn = json.loads(completed_parent["output"])["ExecutionArn"]
    deadline = time.monotonic() + 5
    child_status = s.status(child_arn)
    while child_status["status"] == "RUNNING" and time.monotonic() < deadline:
        time.sleep(0.05)
        child_status = s.status(child_arn)
    assert child_status["status"] == "SUCCEEDED", child_status
    assert s.events(child_arn)[0] == "ExecutionStarted"
    timed_parent = s.machine("sync-slow-lambda", {"StartAt": "Call", "States": {
        "Call": {"Type": "Task", "Resource": sync, "TimeoutSeconds": 1,
                 "Parameters": {"StateMachineArn": blocking_child},
                 "Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "Recovered"}], "End": True},
        "Recovered": {"Type": "Succeed"}}})
    start = time.monotonic()
    timed_arn = s.run(timed_parent, {})
    assert time.monotonic() - start < 1.5
    s.advance(0)
    timed_result = s.status(timed_arn)
    assert timed_result["status"] == "SUCCEEDED" and json.loads(timed_result["output"])["Error"] == "States.Timeout", timed_result


def check_ecs_run_task_sync(s):
    ecs = boto3.client("ecs", endpoint_url=s.url, aws_access_key_id="test", aws_secret_access_key="x",
                       region_name="us-east-1")
    ecs.create_cluster(clusterName="jobs")
    ecs.register_task_definition(family="work", containerDefinitions=[{"name": "main", "image": "busybox",
                                                                      "memory": 64}])
    machine = s.machine("ecs-sync", {"StartAt": "Run", "States": {
        "Run": {"Type": "Task", "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {"Cluster": "jobs", "TaskDefinition": "work"},
                "ResultSelector": {"stopped.$": "$.Tasks[0].LastStatus"}, "ResultPath": "$.run",
                "Catch": [{"ErrorEquals": ["States.TaskFailed"], "ResultPath": "$.err", "Next": "Done"}],
                "Next": "Done"},
        "Done": {"Type": "Succeed"}}})

    def set_status(task_arn, **fields):
        request = urllib.request.Request(
            s.url + "/", data=json.dumps({"cluster": "jobs", "task": task_arn, **fields}).encode(), method="POST",
            headers={"x-amz-target": "AmazonEC2ContainerServiceV20141113.RotoSetTaskStatus",
                     "content-type": "application/x-amz-json-1.1"})
        urllib.request.urlopen(request).read()

    def only_running_task(known):
        tasks = [t for t in ecs.list_tasks(cluster="jobs")["taskArns"] if t not in known]
        assert len(tasks) == 1, tasks
        return tasks[0]

    arn = s.run(machine, {})
    first = only_running_task([])
    s.advance(10)
    assert s.status(arn)["status"] == "RUNNING"  # the task has not stopped
    ecs.stop_task(cluster="jobs", task=first)
    s.advance(5)
    done = s.status(arn)
    assert done["status"] == "SUCCEEDED" and json.loads(done["output"])["run"] == {"stopped": "STOPPED"}, done

    arn = s.run(machine, {})
    second = only_running_task([first])
    set_status(second, lastStatus="STOPPED", exitCode=3)
    s.advance(5)
    out = json.loads(s.status(arn)["output"])
    assert "err" in out and out["err"]["Error"] == "States.TaskFailed", out

    timeout_machine = s.machine("ecs-sync-timeout", {"StartAt": "Run", "States": {
        "Run": {"Type": "Task", "Resource": "arn:aws:states:::ecs:runTask.sync", "TimeoutSeconds": 2,
                "Parameters": {"Cluster": "jobs", "TaskDefinition": "work"},
                "Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "Done"}], "End": True},
        "Done": {"Type": "Succeed"}}})
    timed_arn = s.run(timeout_machine, {})
    timed_task = only_running_task([first, second])
    s.advance(2)
    timed = s.status(timed_arn)
    assert timed["status"] == "SUCCEEDED" and json.loads(timed["output"])["Error"] == "States.Timeout", timed
    assert ecs.describe_tasks(cluster="jobs", tasks=[timed_task])["tasks"][0]["lastStatus"] == "STOPPED"

    aborted_arn = s.run(machine, {})
    aborted_task = only_running_task([first, second, timed_task])
    s.sf.stop_execution(executionArn=aborted_arn)
    assert ecs.describe_tasks(cluster="jobs", tasks=[aborted_task])["tasks"][0]["lastStatus"] == "STOPPED"

    parallel = s.machine("ecs-sync-branch-cancel", {"StartAt": "Both", "States": {"Both": {
        "Type": "Parallel", "End": True, "Branches": [
            {"StartAt": "Run", "States": {"Run": {"Type": "Task", "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {"Cluster": "jobs", "TaskDefinition": "work"}, "End": True}}},
            {"StartAt": "Wait", "States": {"Wait": {"Type": "Wait", "Seconds": 1, "Next": "Fail"},
                "Fail": {"Type": "Fail", "Error": "BranchFailed"}}}]}}})
    parallel_arn = s.run(parallel, {})
    branch_task = only_running_task([first, second, timed_task, aborted_task])
    s.advance(1)
    assert s.status(parallel_arn)["status"] == "FAILED"
    assert ecs.describe_tasks(cluster="jobs", tasks=[branch_task])["tasks"][0]["lastStatus"] == "STOPPED"

    # Without .sync the Task returns right after RunTask.
    plain = s.status(s.run(s.machine("ecs-plain", {"StartAt": "Run", "States": {"Run": {
        "Type": "Task", "Resource": "arn:aws:states:::ecs:runTask",
        "Parameters": {"Cluster": "jobs", "TaskDefinition": "work"}, "TimeoutSeconds": 2, "End": True}}}), {}))
    assert plain["status"] == "SUCCEEDED" and json.loads(plain["output"])["Tasks"][0]["LastStatus"], plain


def check_task_states_call_other_services(s):
    queue_url = s.sqs.get_queue_url(QueueName="jobs")["QueueUrl"]
    s.sqs.purge_queue(QueueUrl=queue_url)
    machine = s.machine("tasks", {"StartAt": "Echo", "States": {
        "Echo": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke",
                 "Parameters": {"FunctionName": "echo", "Payload.$": "$"},
                 "ResultSelector": {"echoed.$": "$.Payload"}, "ResultPath": "$.lambda", "Next": "Direct"},
        "Direct": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:echo",
                   "InputPath": "$.lambda", "ResultPath": "$.direct", "OutputPath": "$", "Next": "Send"},
        "Send": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
                 "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "States.JsonToString($.lambda)"},
                 "ResultSelector": {"sent.$": "$.MessageId"}, "ResultPath": "$.sqs", "End": True}}})
    arn = s.run(machine, {"n": 1})
    done = s.status(arn)
    assert done["status"] == "SUCCEEDED", done
    out = json.loads(done["output"])
    assert out["lambda"] == {"echoed": {"n": 1}}, out
    assert out["sqs"]["sent"], out
    types = s.events(arn)
    assert types.count("LambdaFunctionSucceeded") == 2 and types.count("TaskSucceeded") == 1, types
    assert "TaskScheduled" in types, types
    messages = s.sqs.receive_message(QueueUrl=queue_url)["Messages"]
    assert json.loads(messages[0]["Body"]) == {"echoed": {"n": 1}}, messages


def check_callback_tasks(s):
    queue_url = s.sqs.get_queue_url(QueueName="jobs")["QueueUrl"]
    s.sqs.purge_queue(QueueUrl=queue_url)
    machine = s.machine("callback", {"StartAt": "Ask", "States": {
        "Ask": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
                "TimeoutSeconds": 100, "ResultPath": "$.answer",
                "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "$$.Task.Token"},
                "Catch": [{"ErrorEquals": ["Denied", "States.Timeout"], "ResultPath": "$.err", "Next": "Done"}],
                "Next": "Done"},
        "Done": {"Type": "Succeed"}}})

    def token():
        messages = s.sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)["Messages"]
        s.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])
        return messages[0]["Body"]

    arn = s.run(machine, {"id": 1})
    assert s.status(arn)["status"] == "RUNNING"
    first = token()
    s.sf.send_task_heartbeat(taskToken=first)
    assert code(lambda: s.sf.send_task_success(taskToken=first, output="{")) == "InvalidOutput"
    assert s.status(arn)["status"] == "RUNNING"
    s.sf.send_task_success(taskToken=first, output='{"ok": true}')
    out = json.loads(s.status(arn)["output"])
    assert out == {"id": 1, "answer": {"ok": True}}, out
    assert code(lambda: s.sf.send_task_success(taskToken=first, output="{}")) == "TaskTimedOut"
    assert code(lambda: s.sf.send_task_success(taskToken="x" * 1100, output="{}")) == "InvalidToken"

    arn = s.run(machine, {})
    failure_token = token()
    assert code(lambda: s.sf.send_task_failure(taskToken=failure_token, error="x" * 257)) == "ValidationException"
    assert s.status(arn)["status"] == "RUNNING"
    s.sf.send_task_failure(taskToken=failure_token, error="Denied", cause="no")
    done = s.status(arn)
    assert json.loads(done["output"])["err"] == {"Error": "Denied", "Cause": "no"}, done

    arn = s.run(machine, {})
    stale = token()
    s.advance(101)
    assert json.loads(s.status(arn)["output"])["err"]["Error"] == "States.Timeout"
    assert code(lambda: s.sf.send_task_success(taskToken=stale, output="{}")) == "TaskTimedOut"


def check_activities_and_heartbeats(s):
    activity = s.sf.create_activity(name="worker", tags=[{"key": "team", "value": "a"}])["activityArn"]
    assert s.sf.create_activity(name="worker")["activityArn"] == activity
    tags = lambda: {t["key"]: t["value"] for t in s.sf.list_tags_for_resource(resourceArn=activity)["tags"]}
    assert tags() == {"team": "a"}
    s.sf.tag_resource(resourceArn=activity, tags=[{"key": "env", "value": "dev"}])
    s.sf.untag_resource(resourceArn=activity, tagKeys=["team"])
    assert tags() == {"env": "dev"}
    assert [a["name"] for a in s.sf.list_activities()["activities"]] == ["worker"]
    assert s.sf.describe_activity(activityArn=activity)["name"] == "worker"
    machine = s.machine("activity-flow", {"StartAt": "Work", "States": {
        "Work": {"Type": "Task", "Resource": activity, "TimeoutSeconds": 300, "HeartbeatSeconds": 30,
                 "Parameters": {"job.$": "$.job"}, "ResultPath": "$.result",
                 "Catch": [{"ErrorEquals": ["States.Timeout"], "ResultPath": "$.err", "Next": "Done"}],
                 "Next": "Done"},
        "Done": {"Type": "Succeed"}}})
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(s.sf.get_activity_task, activityArn=activity, workerName="w1")
        time.sleep(0.2)
        assert not pending.done(), "empty activity poll returned immediately"
        first = s.run(machine, {"job": 1})
        task = pending.result(timeout=5)
    second = s.run(machine, {"job": 2})
    assert json.loads(task["input"]) == {"job": 1}, task
    s.advance(20)
    s.sf.send_task_heartbeat(taskToken=task["taskToken"])
    s.advance(20)  # 40s in: alive because the heartbeat moved the deadline
    assert s.status(first)["status"] == "RUNNING"
    s.sf.send_task_success(taskToken=task["taskToken"], output='"done"')
    assert json.loads(s.status(first)["output"])["result"] == "done"
    types = s.events(first)
    assert {"ActivityScheduled", "ActivityStarted", "ActivitySucceeded"} <= set(types), types
    history = {event["type"]: event for event in s.sf.get_execution_history(executionArn=first)["events"]}
    assert history["ActivityStarted"]["previousEventId"] == history["ActivityScheduled"]["id"]
    assert history["ActivitySucceeded"]["previousEventId"] == history["ActivityStarted"]["id"]
    # Its heartbeat clock starts when a worker fetches it, not while it is queued.
    s.advance(100)
    assert s.status(second)["status"] == "RUNNING"
    second_task = s.sf.get_activity_task(activityArn=activity)
    assert json.loads(second_task["input"]) == {"job": 2}
    s.advance(31)
    out = json.loads(s.status(second)["output"])
    assert out["err"]["Error"] == "States.Timeout", out
    third = s.run(machine, {"job": 3})
    third_task = s.sf.get_activity_task(activityArn=activity)
    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = [executor.submit(s.sf.get_activity_task, activityArn=activity) for _ in range(2)]
        time.sleep(0.2)
        assert all(not poll.done() for poll in pending)
        fourth, fifth = s.run(machine, {"job": 4}), s.run(machine, {"job": 5})
        tasks = [poll.result(timeout=5) for poll in pending]
    assert {json.loads(item["input"])["job"] for item in tasks} == {4, 5}, tasks
    assert tasks[0]["taskToken"] != tasks[1]["taskToken"]
    for item in tasks:
        s.sf.send_task_success(taskToken=item["taskToken"], output='"done"')
    assert s.status(fourth)["status"] == s.status(fifth)["status"] == "SUCCEEDED"
    s.sf.delete_activity(activityArn=activity)
    assert code(lambda: s.sf.send_task_heartbeat(taskToken=third_task["taskToken"])) == "TaskDoesNotExist"
    s.sf.stop_execution(executionArn=third)
    assert code(lambda: s.sf.describe_activity(activityArn=activity)) == "ActivityDoesNotExist"


def check_retry_and_catch(s):
    machine = s.machine("recover", {"StartAt": "Boom", "States": {
        "Boom": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:boom",
                 "Retry": [{"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 10, "MaxAttempts": 2,
                            "BackoffRate": 2}],
                 "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.caught", "Next": "Done"}],
                 "Next": "Never"},
        "Never": {"Type": "Fail", "Error": "NotCaught"},
        "Done": {"Type": "Succeed"}}})
    arn = s.run(machine, {"keep": True})
    assert s.status(arn)["status"] == "RUNNING", s.status(arn)  # waiting for the first retry
    s.advance(10)
    assert s.status(arn)["status"] == "RUNNING"  # second retry waits 20s
    s.advance(20)
    done = s.status(arn)
    assert done["status"] == "SUCCEEDED", done
    out = json.loads(done["output"])
    assert out["keep"] is True and out["caught"]["Error"], out
    types = s.events(arn)
    # Three attempts, but the state is entered and exited once: a retry does not re-enter it.
    assert types.count("LambdaFunctionFailed") == 3, types
    assert types.count("TaskStateEntered") == 1 and types.count("TaskStateExited") == 1, types

    # Without a Catch the failure settles the execution, and a missing service fails cleanly.
    plain = s.machine("uncaught", {"StartAt": "Q", "States": {
        "Q": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
              "Parameters": {"QueueUrl": "http://localhost/000000000000/missing", "MessageBody": "x"},
              "End": True}}})
    failed = s.status(s.run(plain))
    assert failed["status"] == "FAILED" and failed["error"].startswith("Sqs."), failed
    unsupported = s.machine("sync", {"StartAt": "Q", "States": {
        "Q": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke.sync", "End": True}}})
    assert s.status(s.run(unsupported))["error"] == "States.Runtime"


def check_history_pages_and_validation(s):
    machine = s.machine("pages", {"StartAt": "A", "States": {
        "A": {"Type": "Pass", "Next": "B"}, "B": {"Type": "Pass", "Next": "C"}, "C": {"Type": "Succeed"}}})
    arn = s.run(machine)
    def invalid_request(operation, body):
        request = urllib.request.Request(
            s.url + "/", data=json.dumps(body).encode(), method="POST",
            headers={"x-amz-target": "AWSStepFunctions." + operation,
                     "content-type": "application/x-amz-json-1.0"})
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as error:
            assert error.code == 400 and "ValidationException" in error.read().decode()
            return
        raise AssertionError("expected ValidationException")

    for value in (-1, 1001, 1.5, True):
        invalid_request("GetExecutionHistory", {"executionArn": arn, "maxResults": value})
        invalid_request("ListActivities", {"maxResults": value})
    for field in ("reverseOrder", "includeExecutionData"):
        invalid_request("GetExecutionHistory", {"executionArn": arn, field: "false"})
    first = s.sf.get_execution_history(executionArn=arn, maxResults=3)
    assert len(first["events"]) == 3 and "nextToken" in first
    events = list(first["events"])
    page = first
    while "nextToken" in page:
        page = s.sf.get_execution_history(executionArn=arn, maxResults=3, nextToken=page["nextToken"])
        events.extend(page["events"])
    ids = [e["id"] for e in events]
    assert ids == list(range(1, len(ids) + 1))
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
        "A": {"Type": "Pass", "Next": "B"},
        "B": {"Type": "Choice", "Choices": [{"Variable": "$.done", "IsPresent": True, "Next": "Done"}], "Default": "A"},
        "Done": {"Type": "Succeed"}}})
    started = time.monotonic()
    arn = s.run(machine)
    s.advance(0)
    done = s.status(arn)
    assert (done["status"], done["error"]) == ("FAILED", "States.Runtime"), done
    assert time.monotonic() - started < 60


def check_update_tags_and_test_state(s):
    old = {"StartAt": "W", "States": {"W": {"Type": "Wait", "Seconds": 600, "Next": "R"},
                                      "R": {"Type": "Pass", "Result": "old", "End": True}}}
    machine = s.sf.create_state_machine(name="evolving", roleArn=ROLE, definition=json.dumps(old),
                                        tags=[{"key": "team", "value": "a"}])["stateMachineArn"]
    running = s.run(machine)
    new = {"StartAt": "R", "States": {"R": {"Type": "Pass", "Result": "new", "End": True}}}
    updated = s.sf.update_state_machine(stateMachineArn=machine, definition=json.dumps(new))
    assert "updateDate" in updated and updated["revisionId"]
    assert json.loads(s.sf.describe_state_machine(stateMachineArn=machine)["definition"]) == new
    assert code(lambda: s.sf.update_state_machine(stateMachineArn=machine)) == "MissingRequiredParameter"
    assert code(lambda: s.sf.update_state_machine(
        stateMachineArn=machine, definition=json.dumps({"StartAt": "X", "States": {}}))) == "InvalidDefinition"
    # A running execution finishes with the definition it started with; new ones use the update.
    s.advance(601)
    assert json.loads(s.status(running)["output"]) == "old"
    assert json.loads(s.status(s.run(machine))["output"]) == "new"
    described = s.sf.describe_state_machine_for_execution(executionArn=running)
    assert json.loads(described["definition"]) == old and described["name"] == "evolving"

    tags = lambda: {t["key"]: t["value"] for t in s.sf.list_tags_for_resource(resourceArn=machine)["tags"]}
    assert tags() == {"team": "a"}
    s.sf.tag_resource(resourceArn=machine, tags=[{"key": "env", "value": "dev"}, {"key": "team", "value": "b"}])
    assert tags() == {"team": "b", "env": "dev"}
    s.sf.untag_resource(resourceArn=machine, tagKeys=["team"])
    assert tags() == {"env": "dev"}
    assert "tags" not in s.sf.describe_state_machine(stateMachineArn=machine)
    assert code(lambda: s.sf.list_tags_for_resource(resourceArn=machine + "x")) == "ResourceNotFound"

    def test_state(state, payload=None):
        return s.sf.test_state(definition=json.dumps(state), roleArn=ROLE, input=json.dumps(payload or {}))

    done = test_state({"Type": "Pass", "Parameters": {"x.$": "$.a"}, "Next": "After"}, {"a": 5})
    assert (done["status"], json.loads(done["output"]), done["nextState"]) == ("SUCCEEDED", {"x": 5}, "After")
    invoked = test_state({"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke",
                          "Parameters": {"FunctionName": "echo", "Payload.$": "$"},
                          "ResultSelector": {"result.$": "$.Payload"}, "End": True}, {"a": 5})
    assert invoked["status"] == "SUCCEEDED" and json.loads(invoked["output"]) == {"result": {"a": 5}}, invoked
    nested = test_state({"Type": "Task", "Resource": "arn:aws:states:::states:startExecution",
                         "Parameters": {"StateMachineArn": machine}, "End": True})
    assert nested["status"] == "SUCCEEDED" and json.loads(nested["output"])["ExecutionArn"], nested
    queue_url = s.sqs.get_queue_url(QueueName="jobs")["QueueUrl"]
    s.sqs.purge_queue(QueueUrl=queue_url)
    callback = {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
                "Parameters": {"QueueUrl": queue_url, "MessageBody.$": "$$.Task.Token"}, "End": True}
    assert test_state(callback)["status"] == "FAILED"
    callback_branch = {"StartAt": "Ask", "States": {"Ask": callback}}
    assert test_state({"Type": "Parallel", "Branches": [callback_branch], "End": True})["status"] == "FAILED"
    assert not s.sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0).get("Messages")
    before = len(s.sf.list_executions(stateMachineArn=machine)["executions"])
    synchronous = {"Type": "Task", "Resource": "arn:aws:states:::states:startExecution.sync",
                   "Parameters": {"StateMachineArn": machine}, "End": True}
    assert test_state(synchronous)["status"] == "FAILED"
    assert len(s.sf.list_executions(stateMachineArn=machine)["executions"]) == before
    assert s.sf.test_state(definition=json.dumps(callback), roleArn=ROLE,
                           mock={"result": "{}"})["status"] == "SUCCEEDED"
    validation = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:sqs:changeMessageVisibilityBatch", "End": True}
    assert code(lambda: s.sf.test_state(definition=json.dumps(validation), mock={"result": "{}"})) == "ValidationException"
    assert s.sf.test_state(definition=json.dumps(validation), mock={"result": "{}", "fieldValidationMode": "PRESENT"})["status"] == "SUCCEEDED"
    assert code(lambda: s.sf.test_state(definition=json.dumps(validation), mock={"result": '{"Successful":"bad"}', "fieldValidationMode": "PRESENT"})) == "ValidationException"
    scaling = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:autoscaling:describeAutoScalingGroups", "End": True}
    assert code(lambda: s.sf.test_state(definition=json.dumps(scaling), mock={"result": "{}"})) == "ValidationException"
    assert s.sf.test_state(definition=json.dumps(scaling), mock={"result": "{}", "fieldValidationMode": "PRESENT"})["status"] == "SUCCEEDED"
    mocked_parallel = {"Type": "Parallel", "Branches": [
        {"StartAt": "Bad", "States": {"Bad": {"Type": "Fail", "Error": "must-not-run"}}},
        {"StartAt": "Bad", "States": {"Bad": {"Type": "Fail", "Error": "must-not-run"}}}],
        "ResultPath": "$.results", "End": True}
    mock_result = s.sf.test_state(definition=json.dumps(mocked_parallel), input='{"keep":true}',
                                  mock={"result": '[1,2]'})
    assert mock_result["status"] == "SUCCEEDED" and json.loads(mock_result["output"]) == {"keep": True, "results": [1, 2]}, mock_result
    assert code(lambda: s.sf.test_state(definition=json.dumps(mocked_parallel), mock={"result": '[1]'})) == "ValidationException"
    error_parallel = {"Type": "Parallel", "Branches": [{"StartAt": "Work", "States": {
        "Work": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage", "End": True}}}],
        "Catch": [{"ErrorEquals": ["States.TaskFailed"], "Next": "Recover"}], "End": True}
    error_args = {"definition": json.dumps(error_parallel), "mock": {"errorOutput": {"error": "SQS.Error"}},
                  "stateConfiguration": {"errorCausedByState": "Work"}}
    caught_child = s.sf.test_state(**error_args)
    assert caught_child["status"] == "CAUGHT_ERROR" and caught_child["nextState"] == "Recover", caught_child
    error_args["stateConfiguration"]["errorCausedByState"] = "Missing"
    assert code(lambda: s.sf.test_state(**error_args)) == "ValidationException"
    distributed = {"Type": "Map", "ItemsPath": "$.items", "ItemProcessor": {
        "ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
        "StartAt": "Done", "States": {"Done": {"Type": "Succeed"}}},
        "ToleratedFailureCount": 1, "ResultWriter": {"Resource": "arn:aws:states:::s3:putObject",
        "Parameters": {"Bucket": "unused"}}, "End": True}
    args = {"definition": json.dumps(distributed), "input": '{"items":[1,2]}',
            "mock": {"result": '{"MapRunArn":"test","ResultWriterDetails":{"Bucket":"b","Key":"k"}}'},
            "stateConfiguration": {"mapIterationFailureCount": 1}, "inspectionLevel": "DEBUG"}
    result = s.sf.test_state(**args)
    assert result["status"] == "SUCCEEDED" and json.loads(result["output"])["ResultWriterDetails"]["Key"] == "k", result
    args["stateConfiguration"]["mapIterationFailureCount"] = 2
    result = s.sf.test_state(**args)
    assert result["status"] == "FAILED" and result["error"] == "States.ExceedToleratedFailureThreshold", result
    distributed["ItemBatcher"] = {"MaxItemsPerBatch": 1, "BatchInput": {"job": "demo"}}
    args["definition"] = json.dumps(distributed)
    args["stateConfiguration"]["mapIterationFailureCount"] = 0
    result = s.sf.test_state(**args)
    batches = json.loads(result["inspectionData"]["afterItemBatcher"])
    assert batches == [{"Items": [1], "BatchInput": {"job": "demo"}},
                       {"Items": [2], "BatchInput": {"job": "demo"}}], result
    distributed.pop("ItemsPath")
    distributed["ItemReader"] = {"Resource": "arn:aws:states:::s3:getObject",
                                 "ReaderConfig": {"InputType": "JSON", "ItemsPointer": "/items"}}
    args["definition"] = json.dumps(distributed)
    args["stateConfiguration"]["mapItemReaderData"] = '{"items":[7,8]}'
    result = s.sf.test_state(**args)
    assert json.loads(result["inspectionData"]["afterItemBatcher"]) == [
        {"Items": [7], "BatchInput": {"job": "demo"}},
        {"Items": [8], "BatchInput": {"job": "demo"}}], result
    distributed["ItemReader"]["ReaderConfig"] = {"InputType": "CSV", "CSVHeaderLocation": "FIRST_ROW"}
    args["definition"] = json.dumps(distributed)
    args["stateConfiguration"]["mapItemReaderData"] = 'name,note\nAda,"hello, world"\n'
    result = s.sf.test_state(**args)
    assert json.loads(result["inspectionData"]["afterItemBatcher"]) == [
        {"Items": [{"name": "Ada", "note": "hello, world"}], "BatchInput": {"job": "demo"}}], result
    distributed["ItemReader"]["ReaderConfig"] = {"InputType": "JSON", "ItemsPointer": "/items"}
    distributed["ItemSelector"] = {"key.$": "$$.Map.Item.Key", "value.$": "$$.Map.Item.Value"}
    args["definition"] = json.dumps(distributed)
    args["stateConfiguration"]["mapItemReaderData"] = '{"items":{"a":1,"b":2}}'
    result = s.sf.test_state(**args)
    assert json.loads(result["inspectionData"]["afterItemSelector"]) == [
        {"key": "a", "value": 1}, {"key": "b", "value": 2}], result
    mocked_callback = {**callback, "ResultSelector": {"answer.$": "$.value"}, "ResultPath": "$.result"}
    mocked = s.sf.test_state(definition=json.dumps(mocked_callback), input='{"before":1}',
                             mock={"fieldValidationMode": "NONE", "result": '{"value":42}'})
    assert mocked["status"] == "SUCCEEDED" and json.loads(mocked["output"]) == {"before": 1, "result": {"answer": 42}}, mocked
    assert not s.sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0).get("Messages")
    assert code(lambda: s.sf.test_state(definition=json.dumps(callback), mock={"fieldValidationMode": "NONE", "result": "{"})) == "ValidationException"
    retrying = {**callback, "Retry": [{"ErrorEquals": ["Denied"], "IntervalSeconds": 2, "BackoffRate": 2, "MaxAttempts": 2}],
                "Catch": [{"ErrorEquals": ["Denied"], "ResultPath": "$.failure", "Next": "Recover"}]}
    mock_error = {"errorOutput": {"error": "Denied", "cause": "no"}}
    retried = s.sf.test_state(definition=json.dumps(retrying), input='{"x":1}', mock=mock_error,
                              stateConfiguration={"retrierRetryCount": 1}, inspectionLevel="DEBUG")
    assert retried["status"] == "RETRIABLE" and retried["inspectionData"]["errorDetails"] == {"retryIndex": 0, "retryBackoffIntervalSeconds": 4}, retried
    assert json.loads(retried["inspectionData"]["input"]) == {"x": 1}, retried
    assert "afterParameters" in retried["inspectionData"], retried
    caught = s.sf.test_state(definition=json.dumps(retrying), input='{"x":1}', mock=mock_error,
                             stateConfiguration={"retrierRetryCount": 2}, inspectionLevel="DEBUG")
    assert caught["status"] == "CAUGHT_ERROR" and caught["nextState"] == "Recover", caught
    assert json.loads(caught["output"]) == {"x": 1, "failure": {"Error": "Denied", "Cause": "no"}}
    assert caught["inspectionData"]["errorDetails"]["catchIndex"] == 0
    failed_mock = s.sf.test_state(definition=json.dumps(callback), mock={"errorOutput": {"error": "Other"}})
    assert failed_mock["status"] == "FAILED" and failed_mock["error"] == "Other"
    assert code(lambda: s.sf.test_state(definition=json.dumps(callback), mock={"result": "{}", **mock_error})) == "ValidationException"
    assert not s.sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0).get("Messages")
    chosen = test_state({"Type": "Choice", "Choices": [{"Variable": "$.n", "NumericEquals": 1, "Next": "One"}],
                         "Default": "Many"}, {"n": 1})
    assert (chosen["status"], chosen["nextState"]) == ("SUCCEEDED", "One")
    waited = test_state({"Type": "Wait", "Seconds": 3600, "End": True}, {"k": 1})
    assert (waited["status"], json.loads(waited["output"])) == ("SUCCEEDED", {"k": 1}) and "nextState" not in waited
    failed = test_state({"Type": "Fail", "Error": "Bad", "Cause": "because"})
    assert (failed["status"], failed["error"], failed["cause"]) == ("FAILED", "Bad", "because")
    branch = {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
    parallel = test_state({"Type": "Parallel", "Branches": [branch], "End": True}, {"n": 1})
    assert parallel["status"] == "SUCCEEDED" and json.loads(parallel["output"]) == [{"n": 1}]
    mapped = test_state({"Type": "Map", "ItemsPath": "$.items", "ItemProcessor": branch, "End": True}, {"items": [1, 2]})
    assert mapped["status"] == "SUCCEEDED" and json.loads(mapped["output"]) == [1, 2]
    broken = test_state({"Type": "Pass", "InputPath": "$.missing", "End": True})
    assert (broken["status"], broken["error"]) == ("FAILED", "States.Runtime")


def check_list_pagination_tokens(s):
    first_activity = s.sf.create_activity(name="page-worker-a")["activityArn"]
    second_activity = s.sf.create_activity(name="page-worker-b")["activityArn"]
    page = s.sf.list_activities(maxResults=1)
    token = page["nextToken"]
    assert len(page["activities"]) == 1
    following = s.sf.list_activities(maxResults=1, nextToken=token)
    assert len(following["activities"]) == 1
    assert {page["activities"][0]["activityArn"], following["activities"][0]["activityArn"]} == {first_activity, second_activity}
    assert code(lambda: s.sf.list_activities(maxResults=2, nextToken=token)) == "InvalidToken"
    assert code(lambda: s.sf.list_state_machines(maxResults=1, nextToken=token)) == "InvalidToken"
    assert len(s.sf.list_activities(maxResults=0)["activities"]) == 2
    s.advance(86401)
    assert code(lambda: s.sf.list_activities(maxResults=1, nextToken=token)) == "InvalidToken"
    s.sf.delete_activity(activityArn=first_activity)
    s.sf.delete_activity(activityArn=second_activity)


def check_list_execution_filters(s):
    machine = s.machine("list-filters", {"StartAt": "Done", "States": {"Done": {"Type": "Succeed"}}})
    execution = s.run(machine)
    assert [item["executionArn"] for item in s.sf.list_executions(stateMachineArn=machine, statusFilter="SUCCEEDED")["executions"]] == [execution]
    assert s.sf.list_executions(stateMachineArn=machine, statusFilter="RUNNING")["executions"] == []
    for extra in ({"statusFilter": "UNKNOWN"}, {"statusFilter": "PENDING_REDRIVE"}, {"redriveFilter": "REDRIVEN"}):
        assert code(lambda: s.sf.list_executions(stateMachineArn=machine, **extra)) == "ValidationException"
    map_run = "arn:aws:states:us-east-1:123456789012:mapRun:example/run"
    assert code(lambda: s.sf.list_executions(stateMachineArn=machine, mapRunArn=map_run)) == "ValidationException"
    assert code(lambda: s.sf.list_executions(mapRunArn=map_run)) == "ResourceNotFound"
    express = s.sf.create_state_machine(name="list-express", roleArn=ROLE, type="EXPRESS",
                                        definition=json.dumps({"StartAt": "Done", "States": {"Done": {"Type": "Succeed"}}}))["stateMachineArn"]
    assert code(lambda: s.sf.list_executions(stateMachineArn=express)) == "StateMachineTypeNotSupported"

    ordered = s.machine("list-order", {"StartAt": "Choose", "States": {
        "Choose": {"Type": "Choice", "Choices": [{"Variable": "$.slow", "BooleanEquals": True, "Next": "Wait"}], "Default": "Done"},
        "Wait": {"Type": "Wait", "Seconds": 10, "Next": "Done"},
        "Done": {"Type": "Succeed"}}})
    older = s.run(ordered, {"slow": True})
    s.advance(2)
    newer = s.run(ordered, {"slow": False})
    s.advance(9)
    results = s.sf.list_executions(stateMachineArn=ordered)["executions"]
    assert [item["executionArn"] for item in results] == [older, newer], results
    assert results[0]["startDate"] < results[1]["startDate"]
    assert results[0]["stopDate"] > results[1]["stopDate"]


def check_sync_express(s):
    standard = s.machine("sync-standard", {"StartAt": "Done", "States": {"Done": {"Type": "Succeed"}}})
    assert code(lambda: s.sf.start_sync_execution(stateMachineArn=standard)) == "StateMachineTypeNotSupported"
    def express(name, definition):
        return s.sf.create_state_machine(name=name, roleArn=ROLE, type="EXPRESS", definition=json.dumps(definition))["stateMachineArn"]
    success = express("sync-success", {"StartAt": "Done", "States": {"Done": {"Type": "Pass", "Result": {"ok": True}, "End": True}}})
    result = s.sf.start_sync_execution(stateMachineArn=success, input='{"x":1}', name="same")
    assert result["status"] == "SUCCEEDED" and json.loads(result["output"]) == {"ok": True}, result
    assert result["name"] == "same" and result["stateMachineArn"] == success and result["stopDate"] >= result["startDate"], result
    failure = express("sync-failure", {"StartAt": "Fail", "States": {"Fail": {"Type": "Fail", "Error": "Oops", "Cause": "bad"}}})
    result = s.sf.start_sync_execution(stateMachineArn=failure)
    assert result["status"] == "FAILED" and result["error"] == "Oops" and result["cause"] == "bad", result
    waiting = express("sync-wait", {"StartAt": "Wait", "States": {
        "Wait": {"Type": "Wait", "Seconds": 5, "Next": "Done"}, "Done": {"Type": "Succeed"}}})
    with ThreadPoolExecutor(max_workers=2) as executor:
        calls = [executor.submit(s.sf.start_sync_execution, stateMachineArn=waiting, name="same", input='{"ok":true}') for _ in range(2)]
        time.sleep(0.4)
        assert all(not call.done() for call in calls)
        s.advance(5)
        results = [call.result(timeout=5) for call in calls]
    assert all(item["status"] == "SUCCEEDED" and json.loads(item["output"]) == {"ok": True} for item in results), results
    assert results[0]["executionArn"] != results[1]["executionArn"]
    too_long = express("sync-timeout", {"StartAt": "Wait", "States": {"Wait": {"Type": "Wait", "Seconds": 400, "End": True}}})
    with ThreadPoolExecutor(max_workers=1) as executor:
        call = executor.submit(s.sf.start_sync_execution, stateMachineArn=too_long)
        time.sleep(0.2)
        assert not call.done()
        s.advance(300)
        result = call.result(timeout=5)
    assert result["status"] == "TIMED_OUT", result


def check_plain_task_timeout(s):
    machine = s.machine("plain-task-timeout", {"StartAt": "Call", "States": {
        "Call": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:slow",
                 "TimeoutSecondsPath": "$.timeout", "Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "Recovered"}], "End": True},
        "Recovered": {"Type": "Succeed"}}})
    start = time.monotonic()
    arn = s.run(machine, {"timeout": 1})
    elapsed = time.monotonic() - start
    result = s.status(arn)
    assert elapsed < 1.8, elapsed
    assert result["status"] == "SUCCEEDED" and json.loads(result["output"])["Error"] == "States.Timeout", result
    assert "LambdaFunctionTimedOut" in s.events(arn), s.events(arn)


def check_rust_services_follow_the_clock(s):
    sqs = boto3.client("sqs", endpoint_url=s.url, aws_access_key_id="test", aws_secret_access_key="x",
                       region_name="us-east-1", config=Config(retries={"max_attempts": 1}))
    queue = sqs.create_queue(QueueName="clocked")["QueueUrl"]

    def receive(timeout=30):
        return sqs.receive_message(QueueUrl=queue, VisibilityTimeout=timeout).get("Messages", [])

    sqs.send_message(QueueUrl=queue, MessageBody="later", DelaySeconds=60)
    assert receive() == []
    s.advance(61)
    assert [m["Body"] for m in receive()] == ["later"]
    assert receive() == []  # in flight
    s.advance(31)  # the visibility timeout has passed
    assert [m["Body"] for m in receive()] == ["later"]
    s.post("/roto-api/clock/reset", {})
    assert s.get("/roto-api/clock")["offset_seconds"] == 0


def main():
    server = Server()
    try:
        for check in (check_pass_and_paths, check_choice_and_fail, check_wait_and_clock,
                      check_parallel_and_map, check_branches_run_concurrently,
                      check_map_max_concurrency_and_distributed, check_nested_executions, check_ecs_run_task_sync,
                      check_task_states_call_other_services,
                      check_callback_tasks, check_activities_and_heartbeats,
                      check_retry_and_catch, check_history_pages_and_validation,
                      check_loops_fail_instead_of_hanging, check_update_tags_and_test_state,
                      check_list_pagination_tokens, check_list_execution_filters,
                      check_sync_express, check_plain_task_timeout, check_rust_services_follow_the_clock):
            started = time.monotonic()
            check(server)
            print(f"{check.__name__}: ok ({time.monotonic() - started:.1f}s)")
        unsupported = json.dumps(server.get("/roto-api/unsupported"))
        assert "GetExecutionHistory" not in unsupported, unsupported
    finally:
        server.stop()


if __name__ == "__main__":
    main()
