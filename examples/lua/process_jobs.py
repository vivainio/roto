import json
import sys

event = json.load(sys.stdin)
for record in event["Records"]:
    print("Processing: " + record["body"], file=sys.stderr)
json.dump({"processed": len(event["Records"])}, sys.stdout)
