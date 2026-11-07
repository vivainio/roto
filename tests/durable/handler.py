"""Real SDK fixture: checkpoint a calculation, suspend, then finish it."""

import json
import os
from pathlib import Path

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration, StepConfig, CallbackConfig, WaitForCallbackConfig
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.waits import WaitForConditionConfig, WaitForConditionDecision


def record(path: str, kind: str) -> None:
    # Test instrumentation: the uncheckpointed handler entry shows replay,
    # while step entries show whether their bodies actually ran again.
    with Path(path).open("a") as log:
        log.write(json.dumps(kind) + "\n")


@durable_step
def calculate(context: StepContext, value: int, audit_path: str, name: str, fail_attempts: int = 0) -> int:
    record(audit_path, name)
    if context.attempt <= fail_attempts:
        raise ValueError("Intentional fixture step failure")
    return value * int(os.environ.get("MULTIPLIER", "2"))


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    if event.get("Records"):
        record_event = event["Records"][0]
        event = json.loads(record_event["body"]) if "body" in record_event else {
            "value": 3, "audit_path": os.environ["AUDIT_PATH"], "wait_seconds": 1,
        }
    elif isinstance(event.get("detail"), dict) and "audit_path" in event["detail"]:
        event = event["detail"]
    record(event["audit_path"], "handler")
    mode = event.get("mode")
    if mode == "wait-for-callback":
        def submit(callback_id, submit_context):
            Path(event["audit_path"] + ".callback").write_text(callback_id)
            record(event["audit_path"], "publish")
        return {"approval": context.wait_for_callback(submit, name="approval", config=WaitForCallbackConfig(
            timeout=Duration.from_seconds(60), heartbeat_timeout=Duration.from_seconds(10),
        ))}
    if mode == "condition":
        def check(state, check_context):
            record(event["audit_path"], f"poll-{state + 1}")
            return state + 1
        result = context.wait_for_condition(check, name="poll", config=WaitForConditionConfig(
            initial_state=0,
            wait_strategy=lambda state, attempt: WaitForConditionDecision(
                should_continue=state < 3, delay=Duration.from_seconds(1),
            ),
        ))
        context.wait(Duration.from_seconds(1), name="after-condition")
        return {"polls": result}
    if mode == "callback":
        callback = context.create_callback("approval", config=CallbackConfig(
            timeout=Duration.from_seconds(event.get("callback_timeout", 60)),
            heartbeat_timeout=Duration.from_seconds(event.get("heartbeat_timeout", 10)),
        ))
        context.step(publish_callback(callback.callback_id, event["audit_path"]), name="publish")
        return {"approval": callback.result()}
    if mode == "contexts":
        def branch(child, value, name):
            result = child.step(calculate(value, event["audit_path"], name), name=name)
            child.wait(Duration.from_seconds(1), name="branch-wait")
            return result
        nested = context.run_in_child_context(lambda child: branch(child, 2, "nested"), name="nested-context")
        mapped = context.map([3, 4], lambda child, value, index, inputs: branch(child, value, f"map-{index}"), name="map")
        parallel = context.parallel([
            lambda child: branch(child, 5, "parallel-a"),
            lambda child: branch(child, 6, "parallel-b"),
        ], name="parallel")
        context.wait(Duration.from_seconds(1), name="final-wait")
        return {"nested": nested, "mapped": mapped.get_results(), "parallel": parallel.get_results()}
    if mode == "invoke":
        result = context.invoke(event["target"], {
            "value": event["value"], "audit_path": event["audit_path"] + ".child", "wait_seconds": 1,
        }, name="child-invoke")
        context.wait(Duration.from_seconds(1), name="after-invoke")
        return {"child": result}
    first = context.step(
        calculate(event["value"], event["audit_path"], "first", event.get("fail_attempts", 0)),
        name="first",
        config=StepConfig(retry_strategy=lambda error, attempt: (
            RetryDecision.retry(Duration.from_seconds(1))
            if attempt < 2 else RetryDecision.no_retry()
        )),
    )
    context.wait(duration=Duration.from_seconds(event.get("wait_seconds", 5)), name="pause")
    second = context.step(
        calculate(first, event["audit_path"], "second"), name="second"
    )
    return {"value": second}


@durable_step
def publish_callback(context: StepContext, callback_id: str, audit_path: str) -> None:
    Path(audit_path + ".callback").write_text(callback_id)
    record(audit_path, "publish")
