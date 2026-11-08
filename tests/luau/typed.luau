--!strict
-- Typed clients use the same service dispatcher as test.call.
test.case("typed sqs and Step Functions clients", function()
    local queue = test.aws.sqs.CreateQueue({QueueName = "typed"})
    local queueUrl = assert(queue.QueueUrl)
    test.aws.sqs.SendMessage({QueueUrl = queueUrl, MessageBody = "hello"})
    local messages = test.aws.sqs.ReceiveMessage({QueueUrl = queueUrl})
    test.equal(assert(messages.Messages)[1].Body, "hello")

    local machine = test.aws.states.CreateStateMachine({
        name = "typed",
        roleArn = "arn:aws:iam::123456789012:role/test",
        definition = '{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}',
    })
    local run = test.aws.states.StartExecution({stateMachineArn = machine.stateMachineArn})
    local result = test.aws.states.DescribeExecution({executionArn = run.executionArn})
    test.equal(result.status, "SUCCEEDED")
end)
