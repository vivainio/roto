#!/usr/bin/env python3
"""SDK smoke for Elastic Load Balancing v2: the Luau control plane and the --alb-port data plane.

Needs boto3 (the .venv-moto environment has it) and a built target/debug/roto-server. Starts its own
server and two local HTTP upstreams, then drives the load balancer the way Terraform or CDK would.
"""
import http.client
import json
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent


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


def eventually(predicate, what, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {what}")


class Upstream:
    """Answers with its name; /healthz follows the `healthy` flag."""

    def __init__(self, name):
        self.name, self.healthy, self.seen = name, True, []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/healthz":
                    self.send_response(200 if outer.healthy else 503)
                    self.end_headers()
                    return
                outer.seen.append({k.lower(): v for k, v in self.headers.items()} | {"path": self.path})
                body = outer.name.encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_POST = do_GET

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


def request(port, host, path, method="GET"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, path, headers={"Host": host})
    response = conn.getresponse()
    body = response.read().decode()
    headers = dict(response.getheaders())
    conn.close()
    return response.status, body, headers


def smoke():
    port, alb_port = free_port(), free_port()
    endpoint = f"http://127.0.0.1:{port}"
    a, b = Upstream("a"), Upstream("b")
    with tempfile.TemporaryDirectory() as temp:
        proc = subprocess.Popen(
            [str(ROOT / "target/debug/roto-server"), "--ephemeral", "--port", str(port),
             "--lua-services", str(ROOT / "services"), "--alb-port", str(alb_port), "--data-dir", temp],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            eventually(lambda: socket.socket().connect_ex(("127.0.0.1", port)) == 0, "server")
            elb = boto3.client("elbv2", endpoint_url=endpoint, region_name="us-east-1",
                               aws_access_key_id="test", aws_secret_access_key="test")
            control_plane(elb)
            data_plane(elb, alb_port, a, b)
        finally:
            proc.terminate()
            proc.wait(10)


def control_plane(elb):
    lb = elb.create_load_balancer(Name="web", Subnets=["subnet-1", "subnet-2"], Tags=[{"Key": "env", "Value": "test"}])["LoadBalancers"][0]
    assert lb["Type"] == "application" and lb["State"]["Code"] == "active"
    assert lb["DNSName"].endswith(".us-east-1.elb.localhost")
    assert code(lambda: elb.create_load_balancer(Name="web", Subnets=["s"])) == "DuplicateLoadBalancerName"
    assert code(lambda: elb.create_load_balancer(Name="-bad-", Subnets=["s"])) == "ValidationError"
    assert code(lambda: elb.describe_load_balancers(Names=["ghost"])) == "LoadBalancerNotFound"
    assert elb.describe_tags(ResourceArns=[lb["LoadBalancerArn"]])["TagDescriptions"][0]["Tags"] == [{"Key": "env", "Value": "test"}]
    attributes = {x["Key"]: x["Value"] for x in elb.describe_load_balancer_attributes(LoadBalancerArn=lb["LoadBalancerArn"])["Attributes"]}
    assert attributes["idle_timeout.timeout_seconds"] == "60"
    elb.modify_load_balancer_attributes(LoadBalancerArn=lb["LoadBalancerArn"], Attributes=[{"Key": "deletion_protection.enabled", "Value": "true"}])
    assert code(lambda: elb.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])) == "OperationNotPermitted"
    elb.modify_load_balancer_attributes(LoadBalancerArn=lb["LoadBalancerArn"], Attributes=[{"Key": "deletion_protection.enabled", "Value": "false"}])

    tg = elb.create_target_group(Name="api", Protocol="HTTP", Port=80, VpcId="vpc-1", TargetType="ip")["TargetGroups"][0]
    assert tg["HealthCheckPath"] == "/" and tg["HealthCheckIntervalSeconds"] == 30 and tg["Matcher"] == {"HttpCode": "200"}
    assert code(lambda: elb.create_target_group(Name="api", Protocol="HTTP", Port=80, VpcId="vpc-1")) == "DuplicateTargetGroupName"
    assert code(lambda: elb.modify_target_group(TargetGroupArn=tg["TargetGroupArn"], HealthCheckIntervalSeconds=5, HealthCheckTimeoutSeconds=5)) == "ValidationError"

    listener = elb.create_listener(
        LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}])["Listeners"][0]
    assert code(lambda: elb.create_listener(LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
                                            DefaultActions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}])) == "DuplicateListener"
    assert code(lambda: elb.create_listener(LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTPS", Port=443,
                                            DefaultActions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}])) == "ValidationError"
    assert code(lambda: elb.delete_target_group(TargetGroupArn=tg["TargetGroupArn"])) == "ResourceInUse"
    assert elb.describe_target_groups(TargetGroupArns=[tg["TargetGroupArn"]])["TargetGroups"][0]["LoadBalancerArns"] == [lb["LoadBalancerArn"]]

    rule = elb.create_rule(
        ListenerArn=listener["ListenerArn"], Priority=5,
        Conditions=[{"Field": "path-pattern", "PathPatternConfig": {"Values": ["/x/*"]}}],
        Actions=[{"Type": "fixed-response", "FixedResponseConfig": {"StatusCode": "200", "MessageBody": "x"}}])["Rules"][0]
    assert rule["Priority"] == "5"
    assert code(lambda: elb.create_rule(ListenerArn=listener["ListenerArn"], Priority=5, Conditions=rule["Conditions"], Actions=rule["Actions"])) == "PriorityInUse"
    rules = elb.describe_rules(ListenerArn=listener["ListenerArn"])["Rules"]
    assert [r["Priority"] for r in rules] == ["5", "default"] and rules[-1]["IsDefault"]
    elb.set_rule_priorities(RulePriorities=[{"RuleArn": rule["RuleArn"], "Priority": 7}])
    assert code(lambda: elb.delete_rule(RuleArn=rules[-1]["RuleArn"])) == "OperationNotPermitted"
    elb.delete_rule(RuleArn=rule["RuleArn"])
    assert [r["Priority"] for r in elb.describe_rules(ListenerArn=listener["ListenerArn"])["Rules"]] == ["default"]

    # Deleting the balancer removes its listener and frees the group.
    elb.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
    assert code(lambda: elb.describe_listeners(ListenerArns=[listener["ListenerArn"]])) == "ListenerNotFound"
    elb.delete_target_group(TargetGroupArn=tg["TargetGroupArn"])


def data_plane(elb, alb_port, a, b):
    lb = elb.create_load_balancer(Name="edge", Subnets=["subnet-1"])["LoadBalancers"][0]
    host = lb["DNSName"]
    groups = {}
    for upstream in (a, b):
        group = elb.create_target_group(
            Name=f"tg-{upstream.name}", Protocol="HTTP", Port=upstream.port, VpcId="vpc-1", TargetType="ip",
            HealthCheckPath="/healthz", HealthCheckIntervalSeconds=5, HealthCheckTimeoutSeconds=2,
            HealthyThresholdCount=2, UnhealthyThresholdCount=2)["TargetGroups"][0]
        elb.register_targets(TargetGroupArn=group["TargetGroupArn"], Targets=[{"Id": "127.0.0.1"}])
        groups[upstream.name] = group["TargetGroupArn"]
    # Targets are not checked until a listener sends traffic to their group.
    listener = elb.create_listener(
        LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
        DefaultActions=[{"Type": "fixed-response", "FixedResponseConfig": {"StatusCode": "404", "MessageBody": "no route"}}])["Listeners"][0]
    elb.create_rule(ListenerArn=listener["ListenerArn"], Priority=1,
                    Conditions=[{"Field": "path-pattern", "Values": ["/a/*"]}],
                    Actions=[{"Type": "forward", "TargetGroupArn": groups["a"]}])
    elb.create_rule(ListenerArn=listener["ListenerArn"], Priority=2,
                    Conditions=[{"Field": "path-pattern", "Values": ["/b/*"]}, {"Field": "http-request-method", "HttpRequestMethodConfig": {"Values": ["GET", "POST"]}}],
                    Actions=[{"Type": "forward", "ForwardConfig": {"TargetGroups": [{"TargetGroupArn": groups["b"], "Weight": 1}]}}])
    elb.create_rule(ListenerArn=listener["ListenerArn"], Priority=3,
                    Conditions=[{"Field": "path-pattern", "PathPatternConfig": {"Values": ["/old/*"]}}],
                    Actions=[{"Type": "redirect", "RedirectConfig": {"Protocol": "HTTPS", "Port": "443", "StatusCode": "HTTP_301"}}])

    assert elb.describe_target_health(TargetGroupArn=groups["a"])["TargetHealthDescriptions"][0]["TargetHealth"]["State"] in ("initial", "healthy")
    status, body, headers = request(alb_port, host, "/a/one")
    assert (status, body) == (200, "a"), (status, body)
    assert request(alb_port, host, "/b/one", "POST")[1] == "b"
    assert request(alb_port, host, "/nothing") [:2] == (404, "no route")
    assert request(alb_port, host, "/old/page")[0] == 301
    assert request(alb_port, "no-such-9.us-east-1.elb.localhost", "/a/x")[0] == 404
    seen = a.seen[-1]
    assert seen["host"] == host and seen["x-forwarded-proto"] == "http" and seen["x-forwarded-port"] == "80", seen
    assert seen["x-forwarded-for"] == "127.0.0.1" and seen["x-amzn-trace-id"].startswith("Root=1-"), seen

    def state(group):
        return elb.describe_target_health(TargetGroupArn=group)["TargetHealthDescriptions"][0]["TargetHealth"]["State"]

    eventually(lambda: state(groups["a"]) == "healthy" and state(groups["b"]) == "healthy", "targets to become healthy")
    a.healthy = False
    eventually(lambda: state(groups["a"]) == "unhealthy", "target a to become unhealthy")
    health = elb.describe_target_health(TargetGroupArn=groups["a"])["TargetHealthDescriptions"][0]["TargetHealth"]
    assert health["Reason"] == "Target.ResponseCodeMismatch" and "503" in health["Description"], health
    a.healthy = True
    eventually(lambda: state(groups["a"]) == "healthy", "target a to recover")

    elb.deregister_targets(TargetGroupArn=groups["b"], Targets=[{"Id": "127.0.0.1", "Port": b.port}])
    assert request(alb_port, host, "/b/one")[0] == 503
    elb.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
    assert request(alb_port, host, "/a/one")[0] in (404, 503)


if __name__ == "__main__":
    smoke()
    print("elbv2 smoke ok")
