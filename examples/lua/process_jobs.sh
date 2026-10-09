#!/bin/sh
# Each invocation receives one JSON event on stdin. Stdout is one JSON result.
exec python3 "$(dirname "$0")/process_jobs.py"
