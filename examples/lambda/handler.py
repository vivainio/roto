"""A command Lambda: JSON in, JSON out, diagnostic logs on stderr."""
import json
import os
import sys

event = json.load(sys.stdin)
print("invocation " + os.environ["ROTO_INVOCATION_ID"], file=sys.stderr)
json.dump({"keys": [r["s3"]["object"]["key"] for r in event.get("Records", [])], "event": event}, sys.stdout)
