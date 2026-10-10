#!/usr/bin/env python3
"""Verify the reusable Lua test-data seed on a fresh, disposable server (stdlib only)."""
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    endpoint = f'http://127.0.0.1:{port}'
    with tempfile.TemporaryFile(mode='w+') as log:
        started = time.monotonic()
        proc = subprocess.Popen([str(root / 'target/debug/roto-server'), '--ephemeral', '--port', str(port),
                                 '--setup', str(root / 'examples/demo/setup.lua')], stdout=log, stderr=log)

        def get(path):
            with urllib.request.urlopen(endpoint + path, timeout=2) as response:
                return json.load(response)

        def resources(service, collection, **query):
            path = f'/roto-api/resources/{service}/{collection}'
            if query:
                path += '?' + urllib.parse.urlencode(query)
            return get(path)

        def eventually(predicate):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError('Demo setup exited before becoming ready')
                try:
                    result = predicate()
                    if result:
                        return result
                except OSError:
                    pass
                time.sleep(.05)
            raise AssertionError('Demo did not become ready')

        def ready():
            with urllib.request.urlopen(endpoint + '/roto-api/health', timeout=1):
                return True

        def completed_deliveries():
            rows = get('/roto-api/events/deliveries')['deliveries']
            return rows if len(rows) == 4 and all(row['state'] == 'succeeded' for row in rows) else None

        try:
            eventually(ready)
            startup = time.monotonic() - started
            expected = [('s3', 'buckets', 3), ('s3', 'objects', 68), ('dynamodb', 'tables', 2),
                        ('dynamodb', 'items', 6), ('sqs', 'queues', 3), ('lambda', 'functions', 2),
                        ('lambda', 'event_source_mappings', 1), ('events', 'buses', 1),
                        ('events', 'rules', 1), ('events', 'targets', 2), ('ssm', 'parameters', 4),
                        ('secretsmanager', 'secrets', 1), ('secretsmanager', 'secret_versions', 2),
                        ('iam', 'users', 1), ('iam', 'roles', 1), ('sns', 'topics', 1), ('sns', 'subscriptions', 1)]
            for service, collection, count in expected:
                assert resources(service, collection)['total'] == count, (service, collection)
            first = resources('s3', 'objects', field='bucket', value='demo-assets')
            second = resources('s3', 'objects', field='bucket', value='demo-assets', offset=50)
            assert first['total'] == 63 and len(first['records']) == 50
            assert len(second['records']) == 13
            rows = first['records'] + second['records']
            assert any(row['key'] == 'notes/a + space ☕.txt' for row in rows)
            archive = resources('s3', 'objects', field='bucket', value='demo-archive')['records']
            releases = [row for row in archive if row['key'] == 'release.json']
            assert len(releases) == 2
            for row in releases:
                path = '/roto-api/s3/object?' + urllib.parse.urlencode({'bucket': row['bucket'], 'key': row['key'], 'version': row['version_id']})
                data = get(path)
                assert data['status'] == ('published' if row['is_latest'] else 'draft')
            assert sum(row['delete_marker'] for row in archive) == 1
            with urllib.request.urlopen(endpoint + '/roto-api/s3/object?bucket=demo-assets&key=reports/large.txt&preview=true') as response:
                assert len(response.read()) == 65536
            items = resources('dynamodb', 'items')['records']
            assert items[0]['item']['shipping']['M']['city']['S'] == 'Helsinki'
            mapping = resources('lambda', 'event_source_mappings')['records'][0]
            assert mapping['config']['State'] == 'Disabled'
            queues = {row['name']: row['id'] for row in resources('sqs', 'queues')['records']}
            messages = resources('sqs', 'messages', field='queue_id', value=queues['demo-orders'])['records']
            assert len(messages) == 3 and all(row['receive_count'] == 0 for row in messages)
            history = get('/roto-api/lambda/invocations')['invocations']
            success = next(row for row in history if row['state'] == 'succeeded')
            failure = next(row for row in history if row['state'] == 'failed')
            assert 'demo-echo' in success['logs']
            assert 'payment service unavailable' in failure['logs']
            deliveries = eventually(completed_deliveries)
            assert len(deliveries) == 4
            assert resources('sqs', 'messages', field='queue_id', value=queues['demo-events'])['total'] == 2
            handoffs = get('/roto-api/s3/notifications')['notifications']
            assert len(handoffs) == 1 and handoffs[0]['target'].endswith(':demo-missing')
            print(f'Lua demo smoke passed: seeded in {startup:.2f}s; 9 services, paginated objects, versions, nested items, retained messages, invocation logs, and delivery history')
        except BaseException:
            log.seek(0)
            print(log.read())
            raise
        finally:
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == '__main__':
    smoke()
