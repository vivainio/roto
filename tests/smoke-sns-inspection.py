#!/usr/bin/env python3
"""SNS SMS history and service health inspection smoke; uses only the Python stdlib."""
import json
import xml.etree.ElementTree as ET
import socket
import sqlite3
import subprocess
import tempfile
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
    data_dir = tempfile.TemporaryDirectory(prefix='roto-sms-inspection-')
    # Start with the previous SNS schema to exercise upgrading an existing store.
    old_schema = (root / 'crates/roto-svc-sns/src/lib.rs').read_text().split('sql: "', 1)[1].split('"', 1)[0]
    with sqlite3.connect(Path(data_dir.name) / 'sns.db') as db:
        db.executescript(old_schema)
        db.execute('CREATE TABLE _roto_migrations (version INTEGER PRIMARY KEY)')
        db.execute('INSERT INTO _roto_migrations VALUES (1)')
    def start():
        return subprocess.Popen([str(root / 'target/debug/roto-server'), '--data-dir', data_dir.name, '--port', str(port)], stdout=subprocess.DEVNULL)
    proc = start()

    def request(path, method='GET', body=None, service=None, target=None, account='test', region='us-east-1'):
        headers = {}
        if service:
            headers['Authorization'] = f'AWS4-HMAC-SHA256 Credential={account}/20261010/{region}/{service}/aws4_request'
        if target:
            headers['X-Amz-Target'] = target
            headers['Content-Type'] = 'application/x-amz-json-1.0'
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        return urllib.request.urlopen(urllib.request.Request(endpoint + path, data=body, headers=headers, method=method), timeout=5)

    def get(path):
        with request(path) as response:
            return json.load(response)

    def call(service, target, body, **scope):
        with request('/', 'POST', body, service, target, **scope) as response:
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
        path = '/roto-api/sns/sms-messages'
        phone = '+15551234567'
        other_phone = '+15557654321'
        def inspect(**filters):
            return get(path + '?' + urllib.parse.urlencode(filters))
        def publish(phone, message, account='111111111111', region='us-east-1', **params):
            payload = urllib.parse.urlencode({'Action': 'Publish', 'PhoneNumber': phone, 'Message': message, **params}).encode()
            with request('/', 'POST', payload, 'sns', account=account, region=region) as response:
                root = ET.fromstring(response.read())
                mid = root.find('.//{*}MessageId')
                assert mid is not None, ET.tostring(root)
                return mid.text
        assert get(path) == {'sms_messages': {}, 'region': 'us-east-1'}
        assert inspect(phoneNumber=phone) == {'sms_messages': {phone: []}, 'region': 'us-east-1'}
        assert inspect(phoneNumber=phone, region='eu-west-1')['region'] == 'eu-west-1'
        fails(path + '?account=invalid', 400)
        first = publish(phone, 'first message', Subject='hello', **{
            'MessageAttributes.entry.1.Name': 'kind',
            'MessageAttributes.entry.1.Value.DataType': 'String',
            'MessageAttributes.entry.1.Value.StringValue': 'inspection',
            'MessageAttributes.entry.2.Name': 'binary',
            'MessageAttributes.entry.2.Value.DataType': 'Binary',
            'MessageAttributes.entry.2.Value.BinaryValue': 'AAEC',
        })
        second = publish(phone, 'second message', account='222222222222', region='eu-west-1')
        third = publish(other_phone, 'other recipient')
        first_record = {
            'PhoneNumber': phone, 'TopicArn': None, 'SubscriptionArn': None,
            'MessageId': first, 'Message': 'first message', 'Subject': 'hello',
            'MessageStructure': None,
            'MessageAttributes': {'kind': {'DataType': 'String', 'StringValue': 'inspection'}, 'binary': {'DataType': 'Binary', 'BinaryValue': 'AAEC'}},
        }
        before = get(path)
        assert before['sms_messages'][phone][0] == first_record
        assert [m['MessageId'] for m in before['sms_messages'][phone]] == [first, second]
        assert before['sms_messages'][other_phone][0]['MessageId'] == third
        # Retained records survive server restarts; reads preserve their order.
        proc.terminate()
        proc.wait(timeout=5)
        proc = start()
        for _ in range(100):
            try:
                request('/roto-api/health').close()
                break
            except OSError:
                if proc.poll() is not None:
                    raise RuntimeError('Server exited after restart')
                time.sleep(.05)
        assert get(path) == before
        stored = get('/roto-api/resources/sns/sms_messages')['records']
        assert len(stored) == 3
        assert stored[0]['record'] == first_record
        assert set(inspect(phoneNumber=phone)['sms_messages']) == {phone}
        assert inspect(account='111111111111', phoneNumber=phone)['sms_messages'][phone] == [first_record]
        assert inspect(region='eu-west-1')['sms_messages'][phone][0]['MessageId'] == second
        assert inspect(account='111111111111', region='eu-west-1', phoneNumber=phone)['sms_messages'] == {phone: []}
        assert inspect(account='999999999999')['sms_messages'] == {}
        assert get(path) == before
        # Publishing to a topic must not appear as a direct-to-phone message.
        payload = urllib.parse.urlencode({'Action': 'CreateTopic', 'Name': 'inspection-topic'}).encode()
        with request('/', 'POST', payload, 'sns') as response:
            arn = ET.fromstring(response.read()).find('.//{*}TopicArn').text
        payload = urllib.parse.urlencode({'Action': 'Publish', 'TopicArn': arn, 'Message': 'topic message'}).encode()
        request('/', 'POST', payload, 'sns').close()
        assert get(path) == before
        health = get('/roto-api/health')
        assert health['services']['sns'] == 'available' and health['services']['sqs'] == 'available'
        assert isinstance(health['version'], str)
        request('/roto-api/reset', 'POST', b'').close()
        assert get(path) == {'sms_messages': {}, 'region': 'us-east-1'}
        assert inspect(phoneNumber=phone)['sms_messages'] == {phone: []}
        publish(phone, 'after reset')
        assert len(inspect(phoneNumber=phone)['sms_messages'][phone]) == 1
        print('SNS inspection smoke passed: schema upgrade, persistent history, attributes, filters, topic exclusion, read-only peeks, reset, and service health')
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        data_dir.cleanup()


if __name__ == '__main__':
    smoke()
