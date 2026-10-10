#!/usr/bin/env python3
"""MiniStack-compatible SQS inspection smoke; uses only the Python stdlib."""
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
        queue = call('sqs', 'AmazonSQS.CreateQueue', {'QueueName': 'inspection-queue'})['QueueUrl']
        message_attrs = {'kind': {'DataType': 'String', 'StringValue': 'inspection'}}
        sent = call('sqs', 'AmazonSQS.SendMessage', {'QueueUrl': queue, 'MessageBody': 'still visible', 'MessageAttributes': message_attrs})
        queue_id = get('/roto-api/resources/sqs/queues')['records'][0]['id']
        messages = get('/roto-api/resources/sqs/messages?' + urllib.parse.urlencode({'field': 'queue_id', 'value': queue_id}))
        assert messages['records'][0]['body'] == 'still visible'
        assert messages['records'][0]['receive_count'] == 0
        account = queue.split('/')[-2]
        peek_path = '/roto-api/sqs/messages'
        def peek(url=queue):
            return get(peek_path + '?' + urllib.parse.urlencode({'QueueUrl': url}))['messages'][account]['us-east-1'][url]
        before = get('/roto-api/resources/sqs/messages')
        message = peek()[0]
        assert message['MessageId'] == sent['MessageId']
        assert message['Body'] == 'still visible' and message['IsVisible']
        assert message['ReceiveCount'] == 0 and message['FirstReceiveTimestamp'] is None
        assert message['MessageAttributes'] == message_attrs
        assert message['MD5OfBody'] == sent['MD5OfMessageBody']
        assert message['MD5OfMessageAttributes'] == sent['MD5OfMessageAttributes']
        assert message['SequenceNumber'] is None
        assert message['SentTimestamp'] == int(message['Attributes']['SentTimestamp']) // 1000
        assert message['Attributes']['SenderId'] == account
        assert peek('http://another-host/' + account + '/inspection-queue') == [message]
        assert get('/roto-api/resources/sqs/messages') == before
        assert get(peek_path + '?account=999999999999') == {'messages': {}}
        assert get(peek_path + '?region=eu-west-1') == {'messages': {}}
        assert get(peek_path + '?QueueUrl=missing') == {'messages': {}}
        fails(peek_path + '?account=invalid', 400)
        assert call('sqs', 'AmazonSQS.ReceiveMessage', {'QueueUrl': queue})['Messages'][0]['Body'] == 'still visible'
        before = get('/roto-api/resources/sqs/messages')
        received = peek()[0]
        assert not received['IsVisible'] and received['ReceiveCount'] == 1
        assert received['FirstReceiveTimestamp'] is not None
        assert get('/roto-api/resources/sqs/messages') == before
        delayed = call('sqs', 'AmazonSQS.SendMessage', {'QueueUrl': queue, 'MessageBody': 'delayed', 'DelaySeconds': 60})
        assert len(peek()) == 2 and not peek()[1]['IsVisible']
        assert peek()[1]['MessageId'] == delayed['MessageId']
        empty = call('sqs', 'AmazonSQS.CreateQueue', {'QueueName': 'inspection-empty'})['QueueUrl']
        assert peek(empty) == []
        fifo = call('sqs', 'AmazonSQS.CreateQueue', {'QueueName': 'inspection.fifo', 'Attributes': {'FifoQueue': 'true'}})['QueueUrl']
        sent_fifo = call('sqs', 'AmazonSQS.SendMessage', {'QueueUrl': fifo, 'MessageBody': 'fifo', 'MessageGroupId': 'group', 'MessageDeduplicationId': 'dedup'})
        fifo_message = peek(fifo)[0]
        assert fifo_message['MessageGroupId'] == 'group' and fifo_message['MessageDeduplicationId'] == 'dedup'
        assert fifo_message['SequenceNumber'] == sent_fifo['SequenceNumber']
        assert set(get(peek_path)['messages'][account]['us-east-1']) == {queue, empty, fifo}
        other = call('sqs', 'AmazonSQS.CreateQueue', {'QueueName': 'inspection-queue'}, account='111111111111', region='eu-west-1')['QueueUrl']
        call('sqs', 'AmazonSQS.SendMessage', {'QueueUrl': other, 'MessageBody': 'other scope'}, account='111111111111', region='eu-west-1')
        all_messages = get(peek_path)['messages']
        assert all_messages['111111111111']['eu-west-1'][other][0]['Body'] == 'other scope'
        assert get(peek_path + '?account=' + account)['messages'] == {account: all_messages[account]}
        assert get(peek_path + '?region=eu-west-1')['messages'] == {'111111111111': all_messages['111111111111']}
        assert get(peek_path + '?' + urllib.parse.urlencode({'QueueUrl': queue, 'region': 'eu-west-1'})) == {'messages': {}}
        print('SQS inspection smoke passed: filters, attributes, FIFO, empty queues, delayed/in-flight messages, and unchanged state')
    finally:
        proc.terminate()
        proc.wait(timeout=5)


if __name__ == '__main__':
    smoke()
