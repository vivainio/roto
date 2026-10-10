#!/usr/bin/env python3
"""Read-only resource browsing and S3 content smoke; uses only the Python stdlib."""
import json
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def smoke():
    root = Path(__file__).resolve().parent.parent
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    endpoint = f'http://127.0.0.1:{port}'
    proc = subprocess.Popen([str(root / 'target/debug/roto-server'), '--ephemeral', '--port', str(port)], stdout=subprocess.DEVNULL)

    def request(path, method='GET', body=None, service=None, target=None):
        headers = {}
        if service:
            headers['Authorization'] = f'AWS4-HMAC-SHA256 Credential=test/20261010/us-east-1/{service}/aws4_request'
        if target:
            headers['X-Amz-Target'] = target
            headers['Content-Type'] = 'application/x-amz-json-1.0'
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        return urllib.request.urlopen(urllib.request.Request(endpoint + path, data=body, headers=headers, method=method), timeout=5)

    def get(path):
        with request(path) as response:
            return json.load(response)

    def call(service, target, body):
        with request('/', 'POST', body, service, target) as response:
            return json.load(response)

    def fails(path, status):
        try:
            request(path)
            raise AssertionError(f'{path} should fail')
        except urllib.error.HTTPError as error:
            assert error.code == status, (path, error.code)

    try:
        for _ in range(100):
            try:
                request('/roto-api/health').close()
                break
            except OSError:
                if proc.poll() is not None:
                    raise RuntimeError('Server exited')
                time.sleep(.05)
        for path in ['/roto-api', '/roto-api/']:
            with request(path) as response:
                assert response.headers.get_content_type() == 'text/html'
                assert b'Browse resources' in response.read()
        # Every advertised collection must be queryable, including WITHOUT ROWID tables.
        for service in get('/roto-api/resources')['services']:
            for collection in service['collections']:
                data = get(f'/roto-api/resources/{service["service"]}/{collection}')
                assert isinstance(data['records'], list)
        request('/inspection-bucket', 'PUT', b'', 's3').close()
        request('/other-bucket', 'PUT', b'', 's3').close()
        keys = ['folder/a +%?#ü.txt', '{}', '../literal.txt', '/leading.txt']
        keys += [f'object-{i:02}' for i in range(51)]
        payload = b'<script>alert("plain text only")</script>\n' + b'x' * 70000
        for key in keys:
            path = '/inspection-bucket/' + urllib.parse.quote(key, safe='')
            request(path, 'PUT', payload if key == keys[0] else b'hello', 's3').close()
        request('/other-bucket/hidden', 'PUT', b'other', 's3').close()
        listing = '/roto-api/resources/s3/objects?' + urllib.parse.urlencode({'field': 'bucket', 'value': 'inspection-bucket'})
        first = get(listing)
        assert first['total'] == 55 and len(first['records']) == 50
        second = get(listing + '&offset=50')
        assert len(second['records']) == 5
        rows = first['records'] + second['records']
        assert {row['key'] for row in rows} == set(keys)
        assert all('path' not in row for row in rows)
        for key in keys[:4]:
            url = '/roto-api/s3/object?' + urllib.parse.urlencode({'bucket': 'inspection-bucket', 'key': key, 'version': 'null'})
            expected = payload if key == keys[0] else b'hello'
            with request(url) as response:
                assert response.read() == expected
                assert response.headers['Content-Disposition'] == 'attachment'
                assert response.headers['Content-Type'] == 'application/octet-stream'
            with request(url + '&preview=true') as response:
                assert response.read() == expected[:65536]
        request('/other-bucket/empty', 'PUT', b'', 's3').close()
        with request('/roto-api/s3/object?bucket=other-bucket&key=empty&preview=true') as response:
            assert response.read() == b''
        request('/other-bucket?versioning', 'PUT', b'<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>', 's3').close()
        versions = []
        for payload_version in [b'first version', b'second version']:
            with request('/other-bucket/versioned', 'PUT', payload_version, 's3') as response:
                versions.append((response.headers['x-amz-version-id'], payload_version))
        objects = get('/roto-api/resources/s3/objects?field=bucket&value=other-bucket')['records']
        assert len([row for row in objects if row['key'] == 'versioned']) == 2
        for version, body in versions:
            with request('/roto-api/s3/object?' + urllib.parse.urlencode({'bucket': 'other-bucket', 'key': 'versioned', 'version': version})) as response:
                assert response.read() == body
        queue = call('sqs', 'AmazonSQS.CreateQueue', {'QueueName': 'inspection-queue'})['QueueUrl']
        call('sqs', 'AmazonSQS.SendMessage', {'QueueUrl': queue, 'MessageBody': 'still visible'})
        queue_id = get('/roto-api/resources/sqs/queues')['records'][0]['id']
        messages = get('/roto-api/resources/sqs/messages?' + urllib.parse.urlencode({'field': 'queue_id', 'value': queue_id}))
        assert messages['records'][0]['body'] == 'still visible'
        assert messages['records'][0]['receive_count'] == 0
        assert call('sqs', 'AmazonSQS.ReceiveMessage', {'QueueUrl': queue})['Messages'][0]['Body'] == 'still visible'
        call('dynamodb', 'DynamoDB_20120810.CreateTable', {'TableName': 'inspection-table', 'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}], 'AttributeDefinitions': [{'AttributeName': 'id', 'AttributeType': 'S'}], 'BillingMode': 'PAY_PER_REQUEST'})
        call('dynamodb', 'DynamoDB_20120810.PutItem', {'TableName': 'inspection-table', 'Item': {'id': {'S': 'one'}, 'value': {'N': '42'}}})
        table_id = get('/roto-api/resources/dynamodb/tables')['records'][0]['table_id']
        items = get('/roto-api/resources/dynamodb/items?' + urllib.parse.urlencode({'field': 'table_id', 'value': table_id}))
        assert items['records'][0]['item']['value']['N'] == '42'
        fails('/roto-api/resources/iam/access_keys', 404)
        fails('/roto-api/resources/s3/objects?offset=-1', 400)
        fails('/roto-api/resources/s3/objects?field=invalid&value=x', 400)
        fails('/roto-api/resources/s3/objects?field=bucket', 400)
        fails('/roto-api/s3/object?bucket=inspection-bucket&key=missing', 404)
        print('Inspection smoke passed: all collections, pagination, scoped contents, unusual keys, downloads, bounded previews, DynamoDB items, and non-consuming SQS reads')
    finally:
        proc.terminate()
        proc.wait(timeout=5)


if __name__ == '__main__':
    smoke()
