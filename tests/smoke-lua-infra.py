#!/usr/bin/env python3
"""boto3 contracts for the infrastructure-shaped services: classic ELB, ELBv2 target health, Auto
Scaling, EC2, Step Functions, SSM Run Command, ECS tasks and inert CloudFormation resources.

Needs boto3 and a built target/debug/roto-server. Starts its own ephemeral server twice, once on the
`services` directory and once on the packaged ZIP bundle, then asserts every call appears in the
trace under the right service and operation and that none lands in /roto-api/unsupported.
"""
import json
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
FORM = {"content-type": "application/x-www-form-urlencoded"}
JSON11 = "application/x-amz-json-1.1"


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
    def __init__(self, bundle):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.proc = subprocess.Popen(
            [str(ROOT / "target/debug/roto-server"), "--port", str(self.port), "--ephemeral",
             "--enable-lua-api", "--lua-services", str(bundle)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), 0.2).close()
                return
            except OSError:
                time.sleep(0.1)
        raise AssertionError("server did not start")

    def stop(self):
        self.proc.terminate()
        self.proc.wait(10)

    def client(self, service, trace):
        return boto3.client(service, endpoint_url=self.url, aws_access_key_id=trace,
                            aws_secret_access_key="x", region_name="us-east-1",
                            config=Config(retries={"max_attempts": 1}))

    def get(self, path):
        with urllib.request.urlopen(self.url + path) as r:
            return json.load(r)

    def lua(self, script):
        request = urllib.request.Request(self.url + "/roto-api/lua", data=script.encode())
        with urllib.request.urlopen(request) as r:
            result = json.load(r)
        assert "error" not in result, result
        return result

    def wire(self, service, headers, body):
        """Run a Roto extension operation from a Luau fixture, as a setup script would."""
        pairs = ",".join(f"[{json.dumps(k)}]={json.dumps(v)}" for k, v in headers.items())
        result = self.lua(f"return roto.wire({json.dumps(service)},'POST','/',"
                          f"{{headers={{{pairs}}},body={json.dumps(body)}}})")
        assert result["result"]["status"] == 200, result

    def form(self, service, body):
        self.wire(service, FORM, body)

    def target(self, service, operation, prefix, payload):
        self.wire(service, {"x-amz-target": f"{prefix}.{operation}", "content-type": JSON11},
                  json.dumps(payload))


def check_elb(s, trace):
    elb, v2 = s.client("elb", trace), s.client("elbv2", trace)
    # Classic and v2 share the signing name; the query Version picks the module.
    elb.create_load_balancer(LoadBalancerName="classic", Tags=[{"Key": "a", "Value": "b"}],
                             Listeners=[{"Protocol": "HTTP", "LoadBalancerPort": 80, "InstancePort": 80}])
    assert code(lambda: elb.create_load_balancer(
        LoadBalancerName="classic", Listeners=[{"Protocol": "HTTP", "LoadBalancerPort": 80, "InstancePort": 80}])
    ) == "DuplicateLoadBalancerName"
    elb.register_instances_with_load_balancer(
        LoadBalancerName="classic", Instances=[{"InstanceId": "i-1"}, {"InstanceId": "i-2"}])
    lb = elb.describe_load_balancers(LoadBalancerNames=["classic"])["LoadBalancerDescriptions"][0]
    assert [i["InstanceId"] for i in lb["Instances"]] == ["i-1", "i-2"]
    assert lb["ListenerDescriptions"][0]["Listener"]["InstancePort"] == 80
    states = elb.describe_instance_health(LoadBalancerName="classic")["InstanceStates"]
    assert {x["State"] for x in states} == {"InService"}
    s.form("elasticloadbalancing", "Action=RotoSetInstanceHealth&Version=2012-06-01&LoadBalancerName=classic"
           "&State=OutOfService&Instances.member.1.InstanceId=i-2")
    states = {x["InstanceId"]: x for x in elb.describe_instance_health(LoadBalancerName="classic")["InstanceStates"]}
    assert states["i-1"]["State"] == "InService" and states["i-2"]["State"] == "OutOfService"
    assert code(lambda: elb.describe_instance_health(
        LoadBalancerName="classic", Instances=[{"InstanceId": "i-9"}])) == "InvalidInstance"
    assert elb.describe_tags(LoadBalancerNames=["classic"])["TagDescriptions"][0]["Tags"] == [{"Key": "a", "Value": "b"}]

    # ELBv2 still answers in the same service, and a target's health can be set with no listener.
    group = v2.create_target_group(Name="tg", Protocol="HTTP", Port=80, VpcId="vpc-1",
                                   TargetType="instance")["TargetGroups"][0]["TargetGroupArn"]
    v2.register_targets(TargetGroupArn=group, Targets=[{"Id": "i-1"}, {"Id": "i-2"}])
    health = v2.describe_target_health(TargetGroupArn=group)["TargetHealthDescriptions"]
    assert {h["TargetHealth"]["State"] for h in health} == {"unused"}
    for state in ["healthy", "unhealthy", "draining", "initial"]:
        s.form("elasticloadbalancing", f"Action=RotoSetTargetHealth&Version=2015-12-01&TargetGroupArn={group}"
               f"&State={state}&Targets.member.1.Id=i-1")
        got = v2.describe_target_health(TargetGroupArn=group, Targets=[{"Id": "i-1"}])
        assert got["TargetHealthDescriptions"][0]["TargetHealth"]["State"] == state
    other = v2.describe_target_health(TargetGroupArn=group, Targets=[{"Id": "i-2"}])
    assert other["TargetHealthDescriptions"][0]["TargetHealth"]["State"] == "unused"

    # A fixture can choose the DNS name of a load balancer instead of the generated one.
    balancer = v2.create_load_balancer(Name="named", Subnets=["subnet-1"])["LoadBalancers"][0]
    assert balancer["DNSName"].startswith("named-")
    s.form("elasticloadbalancing", "Action=RotoSetLoadBalancerDnsName&Version=2015-12-01"
           f"&LoadBalancerArn={balancer['LoadBalancerArn']}&DNSName=internal-named-123456789.elb.example.com")
    described = v2.describe_load_balancers(Names=["named"])["LoadBalancers"][0]
    assert described["DNSName"] == "internal-named-123456789.elb.example.com"


def check_autoscaling(s, trace):
    a = s.client("autoscaling", trace)
    a.create_auto_scaling_group(
        AutoScalingGroupName="g", MinSize=0, MaxSize=4, DesiredCapacity=0, AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="lc", Tags=[{"Key": "env", "Value": "t", "PropagateAtLaunch": True}])
    assert code(lambda: a.create_auto_scaling_group(
        AutoScalingGroupName="g", MinSize=0, MaxSize=1)) == "AlreadyExists"
    a.attach_instances(AutoScalingGroupName="g", InstanceIds=["i-1", "i-2"])
    group = a.describe_auto_scaling_groups(AutoScalingGroupNames=["g"])["AutoScalingGroups"][0]
    assert group["DesiredCapacity"] == 2 and [i["InstanceId"] for i in group["Instances"]] == ["i-1", "i-2"]
    assert group["Tags"][0]["Key"] == "env"
    a.update_auto_scaling_group(AutoScalingGroupName="g", MinSize=1, MaxSize=6, DesiredCapacity=3)
    group = a.describe_auto_scaling_groups()["AutoScalingGroups"][0]
    assert (group["MinSize"], group["MaxSize"], group["DesiredCapacity"]) == (1, 6, 3)
    assert code(lambda: a.update_auto_scaling_group(AutoScalingGroupName="g", MinSize=7)) == "ValidationError"
    a.set_instance_health(InstanceId="i-1", HealthStatus="Unhealthy")
    activities = a.detach_instances(AutoScalingGroupName="g", InstanceIds=["i-2"],
                                    ShouldDecrementDesiredCapacity=True)["Activities"]
    assert activities[0]["StatusCode"] == "Successful"
    group = a.describe_auto_scaling_groups()["AutoScalingGroups"][0]
    assert [(i["InstanceId"], i["HealthStatus"]) for i in group["Instances"]] == [("i-1", "Unhealthy")]
    assert group["DesiredCapacity"] == 2
    a.create_or_update_tags(Tags=[{"ResourceId": "g", "ResourceType": "auto-scaling-group",
                                   "Key": "k", "Value": "v", "PropagateAtLaunch": False}])
    tags = a.describe_tags(Filters=[{"Name": "auto-scaling-group", "Values": ["g"]}])["Tags"]
    assert [(t["Key"], t["Value"]) for t in tags] == [("env", "t"), ("k", "v")]
    a.suspend_processes(AutoScalingGroupName="g", ScalingProcesses=["Launch"])
    suspended = a.describe_auto_scaling_groups()["AutoScalingGroups"][0]["SuspendedProcesses"]
    assert [p["ProcessName"] for p in suspended] == ["Launch"]
    a.resume_processes(AutoScalingGroupName="g")
    assert not a.describe_auto_scaling_groups()["AutoScalingGroups"][0]["SuspendedProcesses"]
    a.put_scheduled_update_group_action(AutoScalingGroupName="g", ScheduledActionName="nightly",
                                        Recurrence="0 3 * * *", MinSize=1)
    actions = a.describe_scheduled_actions(AutoScalingGroupName="g")["ScheduledUpdateGroupActions"]
    assert [x["ScheduledActionName"] for x in actions] == ["nightly"]
    failed = a.batch_delete_scheduled_action(AutoScalingGroupName="g",
                                             ScheduledActionNames=["nightly", "ghost"])["FailedScheduledActions"]
    assert [f["ScheduledActionName"] for f in failed] == ["ghost"]
    assert not a.describe_scheduled_actions(AutoScalingGroupName="g")["ScheduledUpdateGroupActions"]


def check_ec2(s, trace):
    c = s.client("ec2", trace)
    r = boto3.resource("ec2", endpoint_url=s.url, aws_access_key_id=trace, aws_secret_access_key="x",
                       region_name="us-east-1")
    group = c.create_security_group(GroupName="web", Description="d", VpcId="vpc-1")["GroupId"]
    assert code(lambda: c.create_security_group(GroupName="web", Description="d", VpcId="vpc-1")) == "InvalidGroup.Duplicate"
    run = c.run_instances(ImageId="ami-1", MinCount=2, MaxCount=2, InstanceType="t3.micro", SubnetId="subnet-1",
                          SecurityGroupIds=[group],
                          TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": "node"}]}])
    ids = [i["InstanceId"] for i in run["Instances"]]
    assert [i["State"]["Name"] for i in run["Instances"]] == ["pending", "pending"]
    # The next read settles pending -> running.
    found = c.describe_instances(Filters=[{"Name": "tag:Name", "Values": ["no*"]},
                                          {"Name": "instance-state-name", "Values": ["running"]}])
    instances = [i for res in found["Reservations"] for i in res["Instances"]]
    assert sorted(i["InstanceId"] for i in instances) == sorted(ids)
    assert instances[0]["SecurityGroups"][0]["GroupName"] == "web"
    assert instances[0]["Tags"] == [{"Key": "Name", "Value": "node"}]
    assert len(found["Reservations"]) == 1
    # stop: stopping -> stopped; start: pending -> running.
    stop = c.stop_instances(InstanceIds=ids[:1])["StoppingInstances"][0]
    assert (stop["PreviousState"]["Name"], stop["CurrentState"]["Name"]) == ("running", "stopping")
    assert c.describe_instances(InstanceIds=ids[:1])["Reservations"][0]["Instances"][0]["State"]["Name"] == "stopped"
    # Stopping a stopped instance is a no-op; starting a terminated one is refused.
    assert c.stop_instances(InstanceIds=ids[:1])["StoppingInstances"][0]["CurrentState"]["Name"] == "stopped"
    start = c.start_instances(InstanceIds=ids[:1])["StartingInstances"][0]
    assert start["CurrentState"]["Name"] == "pending"
    status = c.describe_instance_status()["InstanceStatuses"]
    assert {x["InstanceId"] for x in status} == set(ids)
    assert {x["InstanceStatus"]["Status"] for x in status} == {"ok"}
    # terminate: shutting-down -> terminated, still visible.
    assert c.terminate_instances(InstanceIds=ids[1:])["TerminatingInstances"][0]["CurrentState"]["Name"] == "shutting-down"
    assert code(lambda: c.start_instances(InstanceIds=ids[1:])) == "IncorrectInstanceState"
    states = {i.id: i.state["Name"] for i in r.instances.all()}
    assert states == {ids[0]: "running", ids[1]: "terminated"}
    assert [i.id for i in r.instances.filter(Filters=[{"Name": "instance-state-name", "Values": ["running"]}])] == ids[:1]
    # Hold a transitional state for a fixture.
    s.form("ec2", f"Action=RotoSetInstanceState&Version=2016-11-15&InstanceId.1={ids[0]}&State=stopping")
    for _ in range(2):
        held = c.describe_instances(InstanceIds=ids[:1])["Reservations"][0]["Instances"][0]
        assert held["State"]["Name"] == "stopping"
    c.stop_instances(InstanceIds=ids[:1])
    assert code(lambda: c.describe_instances(InstanceIds=["i-0123456789abcdef0"])) == "InvalidInstanceID.NotFound"
    assert code(lambda: c.describe_instances(Filters=[{"Name": "bogus", "Values": ["1"]}])) == "InvalidParameterValue"
    assert code(lambda: c.run_instances(ImageId="ami-1", MinCount=1, MaxCount=1, DryRun=True)) == "DryRunOperation"
    groups = c.describe_security_groups(Filters=[{"Name": "group-name", "Values": ["web"]}])["SecurityGroups"]
    assert [g["GroupId"] for g in groups] == [group]
    assert code(lambda: c.delete_security_group(GroupId=group)) == "DependencyViolation"
    c.create_tags(Resources=[group], Tags=[{"Key": "k", "Value": "v"}])
    tags = c.describe_tags(Filters=[{"Name": "resource-id", "Values": [group]}])["Tags"]
    assert [(t["ResourceType"], t["Key"]) for t in tags] == [("security-group", "k")]
    many = [c.run_instances(ImageId="ami-1", MinCount=1, MaxCount=1)["Instances"][0]["InstanceId"] for _ in range(6)]
    page = c.describe_instances(MaxResults=5)
    assert page["NextToken"] and sum(len(x["Instances"]) for x in page["Reservations"]) == 5
    assert many


def check_states(s, trace):
    sf = s.client("stepfunctions", trace)
    machine = sf.create_state_machine(
        name="m", roleArn="arn:aws:iam::123456789012:role/r",
        definition=json.dumps({"StartAt": "A", "States": {"A": {"Type": "Wait", "Seconds": 3600, "End": True}}}))["stateMachineArn"]
    start = sf.start_execution(stateMachineArn=machine, name="e1", input=json.dumps({"a": 1}))
    execution = sf.describe_execution(executionArn=start["executionArn"])
    assert execution["status"] == "RUNNING" and json.loads(execution["input"]) == {"a": 1}
    assert code(lambda: sf.start_execution(stateMachineArn=machine, name="e1", input="{}")) == "ExecutionAlreadyExists"
    sf.stop_execution(executionArn=start["executionArn"], error="E", cause="why")
    stopped = sf.describe_execution(executionArn=start["executionArn"])
    assert (stopped["status"], stopped["error"], stopped["cause"]) == ("ABORTED", "E", "why")
    assert "stopDate" in stopped
    assert len(sf.list_executions(stateMachineArn=machine)["executions"]) == 1
    second = sf.start_execution(stateMachineArn=machine)["executionArn"]
    s.target("states", "RotoCompleteExecution", "AWSStepFunctions",
             {"executionArn": second, "status": "SUCCEEDED", "output": "{}"})
    assert sf.describe_execution(executionArn=second)["status"] == "SUCCEEDED"
    assert code(lambda: sf.describe_execution(
        executionArn=second.replace(":execution:", ":execution:x"))) == "ExecutionDoesNotExist"


def check_ssm(s, trace):
    ssm = s.client("ssm", trace)
    command = ssm.send_command(InstanceIds=["i-1", "i-2"], DocumentName="AWS-RunShellScript",
                               Parameters={"commands": ["echo hi"]})["Command"]
    cid = command["CommandId"]
    assert (command["Status"], command["TargetCount"]) == ("Pending", 2)
    assert code(lambda: ssm.get_command_invocation(CommandId=cid, InstanceId="i-9")) == "InvocationDoesNotExist"
    s.target("ssm", "RotoCompleteCommandInvocation", "AmazonSSM",
             {"CommandId": cid, "InstanceId": "i-1", "Status": "Success", "StandardOutputContent": "hello"})
    s.target("ssm", "RotoCompleteCommandInvocation", "AmazonSSM",
             {"CommandId": cid, "InstanceId": "i-2", "Status": "Failed", "StandardErrorContent": "nope",
              "ResponseCode": 7})
    done = ssm.get_command_invocation(CommandId=cid, InstanceId="i-1")
    assert (done["Status"], done["ResponseCode"], done["StandardOutputContent"]) == ("Success", 0, "hello")
    failed = ssm.get_command_invocation(CommandId=cid, InstanceId="i-2")
    assert (failed["Status"], failed["ResponseCode"], failed["StandardErrorContent"]) == ("Failed", 7, "nope")
    listed = ssm.list_command_invocations(CommandId=cid, Details=True)["CommandInvocations"]
    assert {i["InstanceId"]: i["Status"] for i in listed} == {"i-1": "Success", "i-2": "Failed"}
    assert listed[0]["CommandPlugins"][0]["Name"] == "aws:runShellScript"
    assert ssm.list_commands(CommandId=cid)["Commands"][0]["Status"] == "Failed"
    other = ssm.send_command(InstanceIds=["i-3"], DocumentName="AWS-RunShellScript")["Command"]["CommandId"]
    ssm.cancel_command(CommandId=other)
    assert ssm.list_command_invocations(CommandId=other)["CommandInvocations"][0]["Status"] == "Cancelled"
    assert code(lambda: ssm.send_command(DocumentName="AWS-RunShellScript",
                                         Targets=[{"Key": "tag:a", "Values": ["b"]}])) == "ValidationException"


def check_ecs(s, trace):
    ecs = s.client("ecs", trace)
    ecs.create_cluster(clusterName="c")
    ecs.register_task_definition(family="f", containerDefinitions=[{"name": "app", "image": "img", "memory": 64}])
    task = ecs.run_task(cluster="c", taskDefinition="f", startedBy="me")["tasks"][0]
    assert task["lastStatus"] == "PROVISIONING" and task["containers"][0]["name"] == "app"
    arn = task["taskArn"]
    assert ecs.describe_tasks(cluster="c", tasks=[arn])["tasks"][0]["lastStatus"] == "RUNNING"
    assert ecs.list_tasks(cluster="c")["taskArns"] == [arn]
    s.target("ecs", "RotoSetTaskStatus", "AmazonEC2ContainerServiceV20141113",
             {"cluster": "c", "task": arn, "lastStatus": "STOPPED", "exitCode": 3, "stoppedReason": "boom"})
    described = ecs.describe_tasks(cluster="c", tasks=[arn, "missing"])
    stopped = described["tasks"][0]
    assert (stopped["lastStatus"], stopped["containers"][0]["exitCode"], stopped["stoppedReason"]) == ("STOPPED", 3, "boom")
    assert described["failures"][0]["reason"] == "MISSING"
    assert ecs.list_tasks(cluster="c")["taskArns"] == []
    assert ecs.list_tasks(cluster="c", desiredStatus="STOPPED")["taskArns"] == [arn]


def check_cloudformation(s, trace):
    cf = s.client("cloudformation", trace)
    template = {
        "Metadata": {"Roto::InertResources": True},
        "Resources": {
            "Group": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {"MinSize": "1", "Any": {"Ref": "AWS::Region"}},
                      "Metadata": {"Roto::PhysicalResourceId": "asg-1", "Roto::Attributes": {"Arn": "arn:x"}}},
            "Balancer": {"Type": "AWS::ElasticLoadBalancing::LoadBalancer", "Properties": {}},
            "Queue": {"Type": "AWS::SQS::Queue"},
        },
        "Outputs": {"Name": {"Value": {"Ref": "Group"}}, "Arn": {"Value": {"Fn::GetAtt": ["Group", "Arn"]}}},
    }
    cf.create_stack(StackName="held", TemplateBody=json.dumps(template))
    stack = cf.describe_stacks(StackName="held")["Stacks"][0]
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    assert {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]} == {"Name": "asg-1", "Arn": "arn:x"}
    resources = {r["LogicalResourceId"]: r for r in cf.describe_stack_resources(StackName="held")["StackResources"]}
    assert resources["Group"]["PhysicalResourceId"] == "asg-1"
    assert resources["Balancer"]["ResourceType"] == "AWS::ElasticLoadBalancing::LoadBalancer"
    assert resources["Queue"]["PhysicalResourceId"].startswith("http")
    plain = {"Resources": {"X": {"Type": "AWS::EC2::Instance", "Properties": {}}}}
    assert code(lambda: cf.create_stack(StackName="strict", TemplateBody=json.dumps(plain))) == "ValidationError"
    cf.delete_stack(StackName="held")


CHECKS = [
    ("elasticloadbalancing", check_elb), ("autoscaling", check_autoscaling), ("ec2", check_ec2),
    ("states", check_states), ("ssm", check_ssm), ("ecs", check_ecs), ("cloudformation", check_cloudformation),
]

EXPECTED = {
    "elasticloadbalancing": {"CreateLoadBalancer", "DescribeLoadBalancers", "RegisterInstancesWithLoadBalancer",
                             "DescribeInstanceHealth", "DescribeTargetHealth", "CreateTargetGroup", "RegisterTargets"},
    "autoscaling": {"DescribeAutoScalingGroups", "UpdateAutoScalingGroup", "DescribeTags", "CreateOrUpdateTags",
                    "DetachInstances", "SetInstanceHealth", "ResumeProcesses", "DescribeScheduledActions",
                    "PutScheduledUpdateGroupAction", "BatchDeleteScheduledAction", "CreateAutoScalingGroup"},
    "ec2": {"DescribeInstances", "DescribeInstanceStatus", "StartInstances", "StopInstances", "TerminateInstances",
            "DescribeSecurityGroups", "RunInstances", "CreateTags", "CreateSecurityGroup"},
    "states": {"CreateStateMachine", "StartExecution", "StopExecution", "DescribeExecution"},
    "ssm": {"SendCommand", "ListCommandInvocations", "GetCommandInvocation"},
    "ecs": {"RunTask", "DescribeTasks"},
    "cloudformation": {"CreateStack", "DescribeStacks", "DescribeStackResources"},
}


def run(bundle, label):
    server = Server(bundle)
    try:
        for service, check in CHECKS:
            trace = f"ROTO_infra_{service}"
            check(server, trace)
            seen = {}
            for entry in server.get(f"/roto-api/trace?trace_id={trace}")["entries"]:
                seen.setdefault(entry["service"], set()).add(entry["operation"])
            missing = EXPECTED[service] - seen.get(service, set())
            assert not missing, (label, service, missing, seen)
        unsupported = json.dumps(server.get("/roto-api/unsupported"))
        for service, operations in EXPECTED.items():
            for operation in operations:
                assert f'"{operation}"' not in unsupported, (label, operation)
        print(f"{label}: ok")
    finally:
        server.stop()


def main():
    run(ROOT / "services", "directory")
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "services.zip"
        subprocess.run([sys.executable, str(ROOT / "scripts/package-lua-services.py"), "--output", str(archive)],
                       check=True, stdout=subprocess.DEVNULL)
        run(archive, "zip")


if __name__ == "__main__":
    main()
