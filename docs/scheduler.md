# EventBridge Scheduler

Roto implements the [EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
API (`scheduler`, REST-JSON) and fires schedule targets on time. A schedule sends its
`Input` to an SQS queue or a Lambda function.

```sh
aws --endpoint-url http://localhost:5070 sqs create-queue --queue-name work
aws --endpoint-url http://localhost:5070 scheduler create-schedule \
  --name poll --schedule-expression 'rate(1 minutes)' \
  --flexible-time-window Mode=OFF \
  --target '{"Arn":"arn:aws:sqs:us-east-1:123456789012:work","RoleArn":"arn:aws:iam::123456789012:role/r","Input":"{\"job\":\"poll\",\"tick\":\"<aws.scheduler.scheduled-time>\"}"}'
```

A message appears on `work` about every minute. Configure local SDK credentials and region
`us-east-1` first.

## Schedules

| Field | Behavior |
| --- | --- |
| `ScheduleExpression` | `rate(5 minutes)` (also hours and days), six-field `cron(0 2 * * ? *)` including `?`, `L`, `W`, `#`, names and year ranges, or `at(2026-12-31T23:59:00)` |
| `ScheduleExpressionTimezone` | IANA zone for `cron` and `at`; local times follow daylight saving. A nonexistent local time resolves forward, a repeated one fires once |
| `State` | `DISABLED` schedules never fire |
| `StartDate`, `EndDate` | No firing before or after |
| `Target.Arn` | SQS queue or Lambda function fire; other ARNs are stored, and their deliveries fail |
| `Target.Input` | Delivered verbatim as the SQS message body, or parsed as the Lambda event |
| `ActionAfterCompletion` | `DELETE` removes a one-time `at()` schedule after it fires |
| `FlexibleTimeWindow`, `Target.RoleArn`, `RetryPolicy`, `DeadLetterConfig`, `KmsKeyArn` | Validated or stored, otherwise ignored; schedules fire at the scheduled time |

`Input` may contain `<aws.scheduler.scheduled-time>` (the scheduled time such as
`2026-12-31T23:59:00Z`, not the delivery time), `<aws.scheduler.schedule-arn>`,
`<aws.scheduler.execution-id>` and `<aws.scheduler.attempt-number>`.

Invalid time zones, states and windows are rejected with `ValidationException` when the
schedule is created or updated. An expression roto cannot parse is accepted and stored
(so API-shape tests with placeholder values work) but never fires; check it with
`GetSchedule` rather than expecting an error. Duplicate names give `ConflictException` and
unknown schedules or groups `ResourceNotFoundException`. `rate(1 minutes)` is accepted
even though AWS prefers the singular.

Schedule groups (`CreateScheduleGroup`, `GetScheduleGroup`, `ListScheduleGroups`,
`DeleteScheduleGroup`) and their tags (`TagResource`, `UntagResource`,
`ListTagsForResource`) are supported. The `default` group always exists; deleting a group
deletes its schedules. `ListSchedules` filters on `GroupName`, `NamePrefix` and `State`
and paginates.

## Firing and persistence

The next fire time of every schedule is stored with it. A background ticker queues one
delivery per due, enabled schedule and moves the schedule to its next *future*
occurrence, so a restart neither loses nor repeats a tick, and downtime never backfills
missed ticks. Deliveries use the same persistent queue and three attempts as
[EventBridge](eventbridge.md). A one-time `at()` schedule whose time has already passed
fires on the next tick.

Inspect recent deliveries with:

```sh
curl http://localhost:5070/roto-api/scheduler/deliveries
```

Not implemented: flexible-window jitter, firing target types other than SQS and Lambda, rule
schedules on EventBridge `PutRule`, and typed Luau clients (`roto.aws.scheduler`).
