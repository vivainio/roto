"""Real durable SDK -> Roto checkpoint APIs -> persisted wait and replay."""
import json
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pytest

class Server:
    def __init__(self, directory, handler_name="handler.handler", handler_cwd=None):
        self.directory = directory
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.endpoint = f"http://127.0.0.1:{self.port}"
        self.setup = directory / "setup.luau"
        argv = ", ".join(json.dumps(arg) for arg in [sys.executable, "-m", "roto_aws.lambda_runtime"])
        cwd = json.dumps(str(handler_cwd or Path(__file__).resolve().parent))
        self.setup.write_text(
            '--!strict\n'
            f'roto.lambda.bind("durable-test", {{runtime = {{{argv}, {json.dumps(handler_name)}}}, cwd = {cwd}}})\n'
            f'roto.lambda.bind("plain-test", {{runtime = {{{argv}, "handler.plain_handler"}}, cwd = {cwd}}})\n'
        )
        self.process = None

    def start(self):
        self.log = (self.directory / "server.log").open("a+")
        self.process = subprocess.Popen(
            [str(Path(sys.executable).parent / "roto-server"), "--port", str(self.port),
             "--data-dir", str(self.directory / "store"), "--setup", str(self.setup)],
            stdout=self.log, stderr=self.log,
        )
        try:
            wait_until(lambda: self.healthy(), timeout=15)
        except Exception:
            self.log.seek(0)
            raise AssertionError(self.log.read())
        self.client = boto3.client(
            "lambda", endpoint_url=self.endpoint, region_name="us-east-1",
            aws_access_key_id="testing", aws_secret_access_key="testing",
            config=Config(read_timeout=40, retries={"max_attempts": 0}),
        )

    def healthy(self):
        if self.process.poll() is not None:
            raise AssertionError("Roto exited during startup")
        try:
            with urllib.request.urlopen(self.endpoint + "/roto-api/health", timeout=1):
                return True
        except OSError:
            return False

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
            self.log.close()
            self.process = None

    def create(self, **durable):
        return self.client.create_function(
            FunctionName="durable-test", Role="arn:aws:iam::123456789012:role/local",
            Runtime="provided.al2023", Handler="handler.handler", Code={"ZipFile": b""}, Timeout=30,
            DurableConfig={"ExecutionTimeout": 120, "RetentionPeriodInDays": 1, **durable},
        )

    def history(self):
        with urllib.request.urlopen(self.endpoint + "/roto-api/lambda/invocations", timeout=2) as response:
            return json.load(response)["invocations"]

    def advance(self, seconds):
        request = urllib.request.Request(
            self.endpoint + "/roto-api/clock/advance",
            data=json.dumps({"seconds": seconds}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            return json.load(response)


def wait_until(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for durable execution")


@pytest.fixture
def server(tmp_path):
    server = Server(tmp_path)
    try:
        server.start()
        yield server
    finally:
        server.stop()


def assert_calls(path):
    calls = [json.loads(line) for line in path.read_text().splitlines()]
    assert calls.count("handler") >= 2, calls
    assert calls.count("first") == 1, calls
    assert calls.count("second") == 1, calls


def test_sdk_wait_and_replay_through_roto(server, tmp_path):
    created = server.create()
    assert created["DurableConfig"]["ExecutionTimeout"] == 120
    audit = tmp_path / "calls.jsonl"
    response = server.client.invoke(
        FunctionName="durable-test:$LATEST",
        Payload=json.dumps({"value": 7, "audit_path": str(audit)}).encode(),
    )
    payload = response["Payload"].read()
    assert "FunctionError" not in response, payload
    assert json.loads(payload) == {"value": 28}
    execution = server.client.get_durable_execution(DurableExecutionArn=response["DurableExecutionArn"])
    assert execution["Status"] == "SUCCEEDED", execution
    assert json.loads(execution["Result"]) == {"value": 28}
    assert_calls(audit)
    history = server.history()
    assert any(row["result"] == {"Status": "PENDING"} for row in history), history


def test_sdk_checkpoint_survives_server_restart(server, tmp_path):
    server.create()
    audit = tmp_path / "calls.jsonl"
    payload = json.dumps({"value": 11, "audit_path": str(audit), "wait_seconds": 60}).encode()
    response = server.client.invoke(
        FunctionName="durable-test:$LATEST", InvocationType="Event",
        DurableExecutionName="restart-test", Payload=payload,
    )
    assert response["StatusCode"] == 202
    arn = response["DurableExecutionArn"]
    wait_until(lambda: any(row["result"] == {"Status": "PENDING"} for row in server.history()))
    assert [json.loads(line) for line in audit.read_text().splitlines()] == ["handler", "first"]
    server.stop()
    server.start()
    assert server.client.get_durable_execution(DurableExecutionArn=arn)["Status"] == "RUNNING"
    server.advance(61)
    def completed():
        execution = server.client.get_durable_execution(DurableExecutionArn=arn)
        return execution if execution["Status"] != "RUNNING" else None
    execution = wait_until(completed)
    assert execution["Status"] == "SUCCEEDED", execution
    assert json.loads(execution["Result"]) == {"value": 44}
    assert_calls(audit)
    # Named starts are idempotent, including after completion.
    duplicate = server.client.invoke(
        FunctionName="durable-test:$LATEST", InvocationType="Event",
        DurableExecutionName="restart-test", Payload=payload,
    )
    assert duplicate["DurableExecutionArn"] == arn
    with pytest.raises(ClientError) as error:
        server.client.invoke(
            FunctionName="durable-test:$LATEST", InvocationType="Event",
            DurableExecutionName="restart-test", Payload=b'{"different":true}',
        )
    assert error.value.response["Error"]["Code"] == "ResourceConflictException"


@pytest.mark.parametrize("fail_attempts, expected_status", [(1, "SUCCEEDED"), (2, "FAILED")])
def test_sdk_step_retry_and_failure(server, tmp_path, fail_attempts, expected_status):
    server.create()
    audit = tmp_path / "calls.jsonl"
    response = server.client.invoke(
        FunctionName="durable-test:$LATEST",
        Payload=json.dumps({"value": 3, "audit_path": str(audit), "fail_attempts": fail_attempts}).encode(),
    )
    payload = response["Payload"].read()
    execution = server.client.get_durable_execution(DurableExecutionArn=response["DurableExecutionArn"])
    assert execution["Status"] == expected_status, (execution, payload)
    calls = [json.loads(line) for line in audit.read_text().splitlines()]
    assert calls.count("first") == 2, calls
    if expected_status == "SUCCEEDED":
        assert "FunctionError" not in response, payload
        assert json.loads(payload) == {"value": 12}
        assert calls.count("second") == 1
    else:
        assert response["FunctionError"] == "Unhandled"
        assert "Error" in execution
        assert "second" not in calls


@pytest.mark.parametrize("stop", [False, True])
def test_wait_can_time_out_or_be_stopped(server, tmp_path, stop):
    server.create(ExecutionTimeout=10)
    audit = tmp_path / "calls.jsonl"
    response = server.client.invoke(
        FunctionName="durable-test:$LATEST", InvocationType="Event",
        Payload=json.dumps({"value": 3, "audit_path": str(audit), "wait_seconds": 60}).encode(),
    )
    arn = response["DurableExecutionArn"]
    wait_until(lambda: any(row["result"] == {"Status": "PENDING"} for row in server.history()))
    if stop:
        server.client.stop_durable_execution(DurableExecutionArn=arn)
    server.advance(11)
    def terminal():
        execution = server.client.get_durable_execution(DurableExecutionArn=arn)
        return execution if execution["Status"] != "RUNNING" else None
    execution = wait_until(terminal)
    assert execution["Status"] == ("STOPPED" if stop else "TIMED_OUT")
    assert [json.loads(line) for line in audit.read_text().splitlines()] == ["handler", "first"]


def start_mode(server, tmp_path, mode, **options):
    server.create()
    audit = tmp_path / "calls.jsonl"
    response = server.client.invoke(
        FunctionName="durable-test:$LATEST", InvocationType="Event",
        Payload=json.dumps({"value": 3, "audit_path": str(audit), "mode": mode, **options}).encode(),
    )
    return response["DurableExecutionArn"], audit


def completed(server, arn):
    def terminal():
        result = server.client.get_durable_execution(DurableExecutionArn=arn)
        return result if result["Status"] != "RUNNING" else None
    return wait_until(terminal, timeout=30)


@pytest.mark.parametrize(("action","mode"), [("success","callback"),("failure","callback"),("timeout","callback"),("heartbeat","callback"),("success","wait-for-callback")])
def test_callbacks(server, tmp_path, action, mode):
    arn, audit = start_mode(server, tmp_path, mode)
    token_path = Path(str(audit) + ".callback")
    wait_until(lambda: token_path.exists())
    callback_id = token_path.read_text()
    wait_until(lambda: any(row["result"] == {"Status": "PENDING"} for row in server.history()))
    if action == "success":
        server.stop()
        server.start()
    if action == "heartbeat":
        server.advance(6)
        server.client.send_durable_execution_callback_heartbeat(CallbackId=callback_id)
        server.advance(6)
        assert server.client.get_durable_execution(DurableExecutionArn=arn)["Status"] == "RUNNING"
    if action in ("success", "heartbeat"):
        server.client.send_durable_execution_callback_success(CallbackId=callback_id, Result=b'{"approved":true}')
    elif action == "failure":
        server.client.send_durable_execution_callback_failure(CallbackId=callback_id, Error={"ErrorType":"Rejected","ErrorMessage":"No"})
    else:
        server.advance(11)
    result = completed(server, arn)
    assert result["Status"] == ("SUCCEEDED" if action in ("success", "heartbeat") else "FAILED"), result
    if result["Status"] == "SUCCEEDED":
        assert json.loads(result["Result"]) == {"approval": '{"approved":true}'}
    calls = [json.loads(line) for line in audit.read_text().splitlines()]
    assert calls.count("publish") == 1
    with pytest.raises(ClientError):
        server.client.send_durable_execution_callback_success(CallbackId=callback_id, Result=b'null')


def test_nested_contexts_map_and_parallel_replay(server, tmp_path):
    arn, audit = start_mode(server, tmp_path, "contexts")
    result = completed(server, arn)
    assert result["Status"] == "SUCCEEDED", result
    assert json.loads(result["Result"]) == {"nested":4,"mapped":[6,8],"parallel":[10,12]}
    calls = [json.loads(line) for line in audit.read_text().splitlines()]
    for name in ["nested", "map-0", "map-1", "parallel-a", "parallel-b"]:
        assert calls.count(name) == 1, calls


@pytest.mark.parametrize("target", ["durable-test:$LATEST", "plain-test"])
def test_chained_invocations(server, tmp_path, target):
    arn, _ = start_mode(server, tmp_path, "invoke", target=target)
    if target == "plain-test":
        server.client.create_function(FunctionName="plain-test",Role="arn:aws:iam::123456789012:role/local",Code={"ZipFile":b""})
    result = completed(server, arn)
    assert result["Status"] == "SUCCEEDED", result
    assert json.loads(result["Result"]) == {"child":{"value":12 if target.startswith("durable") else 30}}


def test_history_listing_and_retention(server, tmp_path):
    arn, _ = start_mode(server, tmp_path, "contexts")
    assert completed(server,arn)["Status"] == "SUCCEEDED"
    listed = server.client.list_durable_executions_by_function(FunctionName="durable-test", Statuses=["SUCCEEDED"])
    assert [row["DurableExecutionArn"] for row in listed["DurableExecutions"]] == [arn]
    events = []
    marker = None
    while True:
        page = server.client.get_durable_execution_history(DurableExecutionArn=arn, MaxItems=2, IncludeExecutionData=True, **({"Marker":marker} if marker else {}))
        events.extend(page["Events"])
        marker = page.get("NextMarker")
        if not marker:
            break
    assert [event["EventId"] for event in events] == list(range(1,len(events)+1))
    assert events[0]["EventType"] == "ExecutionStarted"
    assert events[-1]["EventType"] == "ExecutionSucceeded"
    server.advance(86401)
    def gone():
        try:
            server.client.get_durable_execution(DurableExecutionArn=arn)
        except ClientError as error:
            return error.response["Error"]["Code"] == "ResourceNotFoundException"
        return False
    wait_until(gone)


def test_alias_execution_is_pinned_to_published_version(server, tmp_path):
    server.create()
    v1 = server.client.publish_version(FunctionName="durable-test")
    assert v1["Version"] == "1"
    alias = server.client.create_alias(FunctionName="durable-test", Name="live", FunctionVersion="1")
    assert alias["FunctionVersion"] == "1"
    audit = tmp_path / "version-calls.jsonl"
    response = server.client.invoke(
        FunctionName="durable-test:live", InvocationType="Event",
        Payload=json.dumps({"value":3,"audit_path":str(audit),"wait_seconds":60}).encode(),
    )
    arn = response["DurableExecutionArn"]
    assert ":durable-test:1/durable-execution/" in arn
    wait_until(lambda:any(row["result"] == {"Status":"PENDING"} for row in server.history()))
    server.client.update_function_configuration(FunctionName="durable-test",Environment={"Variables":{"MULTIPLIER":"3"}})
    v2 = server.client.publish_version(FunctionName="durable-test")
    assert v2["Version"] == "2"
    server.client.update_alias(FunctionName="durable-test", Name="live", FunctionVersion="2", RevisionId=alias["RevisionId"])
    server.stop()
    server.start()
    assert server.client.get_alias(FunctionName="durable-test",Name="live")["FunctionVersion"] == "2"
    server.advance(61)
    result = completed(server,arn)
    assert result["Status"] == "SUCCEEDED",result
    assert json.loads(result["Result"]) == {"value":12}
    assert result["Version"] == "1"
    response = server.client.invoke(
        FunctionName="durable-test",Qualifier="live",
        Payload=json.dumps({"value":3,"audit_path":str(tmp_path/"v2.jsonl"),"wait_seconds":1}).encode(),
    )
    payload = response["Payload"].read()
    assert "FunctionError" not in response,payload
    assert json.loads(payload) == {"value":27}
    assert response["ExecutedVersion"] == "2"
    versions = server.client.list_versions_by_function(FunctionName="durable-test")
    assert [v["Version"] for v in versions["Versions"]] == ["$LATEST","1","2"]
    assert len(server.client.list_functions()["Functions"]) == 1
    assert len(server.client.list_aliases(FunctionName="durable-test")["Aliases"]) == 1
    server.client.delete_alias(FunctionName="durable-test",Name="live")
    assert server.client.list_aliases(FunctionName="durable-test").get("Aliases",[]) == []


def service_client(server, service):
    return boto3.client(service,endpoint_url=server.endpoint,region_name="us-east-1",aws_access_key_id="testing",aws_secret_access_key="testing")


def test_sqs_event_source_waits_for_durable_result(server, tmp_path):
    server.create()
    audit = tmp_path / "sqs.jsonl"
    sqs = service_client(server,"sqs")
    queue = sqs.create_queue(QueueName="durable-input",Attributes={"VisibilityTimeout":"60"})["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue,AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    server.client.create_event_source_mapping(FunctionName="durable-test",EventSourceArn=queue_arn,BatchSize=1)
    sqs.send_message(QueueUrl=queue,MessageBody=json.dumps({"value":5,"audit_path":str(audit),"wait_seconds":1}))
    def done():
        rows = server.client.list_durable_executions_by_function(FunctionName="durable-test",Statuses=["SUCCEEDED"]).get("DurableExecutions",[])
        return rows[0] if rows else None
    execution = wait_until(done)
    assert json.loads(server.client.get_durable_execution(DurableExecutionArn=execution["DurableExecutionArn"])["Result"]) == {"value":20}
    def drained():
        attributes=sqs.get_queue_attributes(QueueUrl=queue,AttributeNames=["ApproximateNumberOfMessages","ApproximateNumberOfMessagesNotVisible"])["Attributes"]
        return attributes["ApproximateNumberOfMessages"]=="0" and attributes["ApproximateNumberOfMessagesNotVisible"]=="0"
    wait_until(drained)
    assert_calls(audit)


@pytest.mark.parametrize("source",["events","s3"])
def test_service_events_start_durable_executions(server,tmp_path,source):
    function=server.create()
    audit=tmp_path/"source.jsonl"
    if source=="events":
        events=service_client(server,"events")
        events.put_rule(Name="durable-rule",EventPattern=json.dumps({"source":["test.durable"]}))
        events.put_targets(Rule="durable-rule",Targets=[{"Id":"workflow","Arn":function["FunctionArn"]}])
        response=events.put_events(Entries=[{"Source":"test.durable","DetailType":"test","Detail":json.dumps({"value":3,"audit_path":str(audit),"wait_seconds":1})}])
        assert response["FailedEntryCount"]==0
    else:
        server.client.update_function_configuration(FunctionName="durable-test",Environment={"Variables":{"AUDIT_PATH":str(audit)}})
        s3=service_client(server,"s3")
        s3.create_bucket(Bucket="durable-bucket")
        s3.put_bucket_notification_configuration(Bucket="durable-bucket",NotificationConfiguration={"LambdaFunctionConfigurations":[{"Id":"workflow","LambdaFunctionArn":function["FunctionArn"],"Events":["s3:ObjectCreated:*"]}]})
        s3.put_object(Bucket="durable-bucket",Key="test",Body=b"payload")
    def done():
        rows=server.client.list_durable_executions_by_function(FunctionName="durable-test",Statuses=["SUCCEEDED"]).get("DurableExecutions",[])
        return rows[0] if rows else None
    wait_until(done)
    assert_calls(audit)


def test_wait_for_condition_preserves_poll_state(server,tmp_path):
    arn,audit=start_mode(server,tmp_path,"condition")
    result=completed(server,arn)
    assert result["Status"]=="SUCCEEDED",result
    assert json.loads(result["Result"])=={"polls":3}
    calls=[json.loads(line) for line in audit.read_text().splitlines()]
    assert [call for call in calls if call.startswith("poll-")]==["poll-1","poll-2","poll-3"]
