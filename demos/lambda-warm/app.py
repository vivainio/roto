import time

invocations = 0


def handler(event, context):
    global invocations
    invocations += 1
    print(f"warm-demo: {event.get('label', '')}", flush=True)
    time.sleep(event.get("sleep", 0))
    if event.get("fail"):
        raise ValueError("intentional handler failure")
    return {"count": invocations, "requestId": context.aws_request_id}
