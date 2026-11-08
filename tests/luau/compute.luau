--!strict
local function call(service, op, input) return test.call(service, op, input) end
local function fails(service, op, input, code)
    test.expect_error(code, function() return test.try_call(service, op, input) end)
end

test.suite("Auto Scaling groups", function(suite)
    local function a(op, input) return call("autoscaling", op, input) end
    suite.before_all(function()
        a("CreateAutoScalingGroup", {AutoScalingGroupName = "g", MinSize = 0, MaxSize = 4, DesiredCapacity = 0,
            AvailabilityZones = {"us-east-1a"}, LaunchConfigurationName = "lc", Tags = {{Key = "env", Value = "t", PropagateAtLaunch = true}}})
    end)
    suite.case("attaches and detaches instances, capacities and health", function()
        fails("autoscaling", "CreateAutoScalingGroup", {AutoScalingGroupName = "g", MinSize = 0, MaxSize = 1}, "AlreadyExists")
        a("AttachInstances", {AutoScalingGroupName = "g", InstanceIds = {"i-1", "i-2"}})
        local group = a("DescribeAutoScalingGroups", {AutoScalingGroupNames = {"g"}}).AutoScalingGroups[1]
        test.equal(group.DesiredCapacity, 2)
        test.equal({group.Instances[1].InstanceId, group.Instances[2].InstanceId}, {"i-1", "i-2"})
        test.equal(group.Tags[1].Key, "env")
        a("UpdateAutoScalingGroup", {AutoScalingGroupName = "g", MinSize = 1, MaxSize = 6, DesiredCapacity = 3})
        group = a("DescribeAutoScalingGroups", {}).AutoScalingGroups[1]
        test.equal({group.MinSize, group.MaxSize, group.DesiredCapacity}, {1, 6, 3})
        fails("autoscaling", "UpdateAutoScalingGroup", {AutoScalingGroupName = "g", MinSize = 7}, "ValidationError")
        a("SetInstanceHealth", {InstanceId = "i-1", HealthStatus = "Unhealthy"})
        local activities = a("DetachInstances", {AutoScalingGroupName = "g", InstanceIds = {"i-2"}, ShouldDecrementDesiredCapacity = true}).Activities
        test.equal(activities[1].StatusCode, "Successful")
        group = a("DescribeAutoScalingGroups", {}).AutoScalingGroups[1]
        test.equal(group.Instances[1].InstanceId, "i-1"); test.equal(group.Instances[1].HealthStatus, "Unhealthy")
        test.equal(#group.Instances, 1); test.equal(group.DesiredCapacity, 2)
    end)
    suite.case("updates group tags and process suspension", function()
        a("CreateOrUpdateTags", {Tags = {{ResourceId = "g", ResourceType = "auto-scaling-group", Key = "k", Value = "v", PropagateAtLaunch = false}}})
        local tags = a("DescribeTags", {Filters = {{Name = "auto-scaling-group", Values = {"g"}}}}).Tags
        test.equal({tags[1].Key, tags[1].Value, tags[2].Key, tags[2].Value}, {"env", "t", "k", "v"})
        a("SuspendProcesses", {AutoScalingGroupName = "g", ScalingProcesses = {"Launch"}})
        test.equal(a("DescribeAutoScalingGroups", {}).AutoScalingGroups[1].SuspendedProcesses[1].ProcessName, "Launch")
        a("ResumeProcesses", {AutoScalingGroupName = "g"})
        test.equal(a("DescribeAutoScalingGroups", {}).AutoScalingGroups[1].SuspendedProcesses, {})
    end)
    suite.case("creates and batch deletes scheduled actions", function()
        a("PutScheduledUpdateGroupAction", {AutoScalingGroupName = "g", ScheduledActionName = "nightly", Recurrence = "0 3 * * *", MinSize = 1})
        test.equal(a("DescribeScheduledActions", {AutoScalingGroupName = "g"}).ScheduledUpdateGroupActions[1].ScheduledActionName, "nightly")
        local failed = a("BatchDeleteScheduledAction", {AutoScalingGroupName = "g", ScheduledActionNames = {"nightly", "ghost"}}).FailedScheduledActions
        test.equal(#failed, 1); test.equal(failed[1].ScheduledActionName, "ghost")
        test.equal(a("DescribeScheduledActions", {AutoScalingGroupName = "g"}).ScheduledUpdateGroupActions, {})
    end)
end)

test.suite("EC2 instances and security groups", function(suite)
    local group = ""
    local function c(op, input) return call("ec2", op, input) end
    suite.before_all(function()
        group = c("CreateSecurityGroup", {GroupName = "web", Description = "d", VpcId = "vpc-1"}).GroupId
    end)
    suite.case("runs, stops, starts and terminates instances", function()
        fails("ec2", "CreateSecurityGroup", {GroupName = "web", Description = "d", VpcId = "vpc-1"}, "InvalidGroup.Duplicate")
        local run = c("RunInstances", {ImageId = "ami-1", MinCount = 2, MaxCount = 2, InstanceType = "t3.micro", SubnetId = "subnet-1", SecurityGroupIds = {group},
            TagSpecifications = {{ResourceType = "instance", Tags = {{Key = "Name", Value = "node"}}}}})
        local one, two = run.Instances[1].InstanceId, run.Instances[2].InstanceId
        test.equal({run.Instances[1].State.Name, run.Instances[2].State.Name}, {"pending", "pending"})
        local found = c("DescribeInstances", {Filters = {{Name = "tag:Name", Values = {"no*"}}, {Name = "instance-state-name", Values = {"running"}}}}).Reservations
        test.equal(#found, 1); test.equal(#found[1].Instances, 2)
        test.equal(found[1].Instances[1].SecurityGroups[1].GroupName, "web")
        test.equal(found[1].Instances[1].Tags, {{Key = "Name", Value = "node"}})
        local stopped = c("StopInstances", {InstanceIds = {one}}).StoppingInstances[1]
        test.equal({stopped.PreviousState.Name, stopped.CurrentState.Name}, {"running", "stopping"})
        test.equal(c("DescribeInstances", {InstanceIds = {one}}).Reservations[1].Instances[1].State.Name, "stopped")
        test.equal(c("StopInstances", {InstanceIds = {one}}).StoppingInstances[1].CurrentState.Name, "stopped")
        test.equal(c("StartInstances", {InstanceIds = {one}}).StartingInstances[1].CurrentState.Name, "pending")
        local status = c("DescribeInstanceStatus", {}).InstanceStatuses
        test.equal(#status, 2); test.equal(status[1].InstanceStatus.Status, "ok"); test.equal(status[2].InstanceStatus.Status, "ok")
        test.equal(c("TerminateInstances", {InstanceIds = {two}}).TerminatingInstances[1].CurrentState.Name, "shutting-down")
        fails("ec2", "StartInstances", {InstanceIds = {two}}, "IncorrectInstanceState")
        local states = {}
        for _, reservation in c("DescribeInstances", {}).Reservations do
            for _, instance in reservation.Instances do states[instance.InstanceId] = instance.State.Name end
        end
        test.equal(states[one], "running"); test.equal(states[two], "terminated")
        local running = c("DescribeInstances", {Filters = {{Name = "instance-state-name", Values = {"running"}}}}).Reservations
        test.equal(#running, 1); test.equal(#running[1].Instances, 1); test.equal(running[1].Instances[1].InstanceId, one)
        local held = c("RotoSetInstanceState", {InstanceIds = {one}, State = "stopping"}); assert(held)
        for _ = 1, 2 do test.equal(c("DescribeInstances", {InstanceIds = {one}}).Reservations[1].Instances[1].State.Name, "stopping") end
        c("StopInstances", {InstanceIds = {one}})
        fails("ec2", "DescribeInstances", {InstanceIds = {"i-0123456789abcdef0"}}, "InvalidInstanceID.NotFound")
        fails("ec2", "DescribeInstances", {Filters = {{Name = "bogus", Values = {"1"}}}}, "InvalidParameterValue")
        fails("ec2", "RunInstances", {ImageId = "ami-1", MinCount = 1, MaxCount = 1, DryRun = true}, "DryRunOperation")
        fails("ec2", "DeleteSecurityGroup", {GroupId = group}, "DependencyViolation")
    end)
    suite.case("filters and tags security groups", function()
        local groups = c("DescribeSecurityGroups", {Filters = {{Name = "group-name", Values = {"web"}}}}).SecurityGroups
        test.equal(#groups, 1); test.equal(groups[1].GroupId, group)
        c("CreateTags", {Resources = {group}, Tags = {{Key = "k", Value = "v"}}})
        local tags = c("DescribeTags", {Filters = {{Name = "resource-id", Values = {group}}}}).Tags
        test.equal({tags[1].ResourceType, tags[1].Key, tags[1].Value}, {"security-group", "k", "v"})
    end)
    suite.case("paginates instances", function()
        for _ = 1, 6 do c("RunInstances", {ImageId = "ami-1", MinCount = 1, MaxCount = 1}) end
        local page = c("DescribeInstances", {MaxResults = 5}); assert(page.NextToken)
        local count = 0
        for _, reservation in page.Reservations do count += #reservation.Instances end
        test.equal(count, 5)
    end)
end)
