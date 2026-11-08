#!/usr/bin/env python3
# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""HTTP and SDK smoke checks for Luau services and their bundle lifecycle.

Checks directory and ZIP bundles, account and region routing, restart and reset.
Behavioral scenarios live in tests/luau; --sdk additionally requires boto3.
"""
import argparse
import datetime
import json
import os
import sys
import zipfile
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = {
    'cur': 'AWSOrigamiServiceGatewayService', 'acm': 'CertificateManager',
    'budgets': 'AWSBudgetServiceGateway', 'config': 'StarlingDoveService',
    'servicediscovery': 'Route53AutoNaming_v20170314',
    'ecr': 'AmazonEC2ContainerRegistry_V20150921', 'ecs': 'AmazonEC2ContainerServiceV20141113',
    'glue': 'AWSGlue', 'wafv2': 'AWSWAF_20190729',
}
ACCOUNT = '123456789012'


def validate_output(value, shape, path='response'):
    # Shared botocore shapes can carry request-only minimum lengths (e.g. ACM
    # Tags requires one tag on input, but an untagged certificate returns []).
    # Check wire field names and types without imposing those input constraints.
    kind = shape.type_name
    if kind == 'structure':
        assert isinstance(value, dict), (path, value)
        assert not set(value).difference(shape.members), (path, set(value).difference(shape.members))
        for key, item in value.items():
            validate_output(item, shape.members[key], path + '.' + key)
    elif kind == 'list':
        assert isinstance(value, list), (path, value)
        for item in value:
            validate_output(item, shape.member, path + '[]')
    elif kind == 'map':
        assert isinstance(value, dict), (path, value)
        for key, item in value.items():
            validate_output(key, shape.key, path + '.key')
            validate_output(item, shape.value, path + '.' + key)
    elif kind in ('string', 'blob'):
        assert isinstance(value, str), (path, value)
    elif kind == 'boolean':
        assert isinstance(value, bool), (path, value)
    elif kind in ('integer', 'long'):
        assert isinstance(value, int) and not isinstance(value, bool), (path, value)
    elif kind in ('float', 'double', 'timestamp'):
        assert isinstance(value, (int, float)) and not isinstance(value, bool), (path, value)


class Server:
    def __init__(self, directory, bundle=None, sdk=False, environment_bundle=None):
        self.sdk = sdk
        self.seen = set()
        if sdk:
            import botocore.session
            self.models = botocore.session.get_session()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}'
        args = [str(ROOT / 'target/debug/roto-server'), '--data-dir', str(directory), '--port', str(port), '--account-id', ACCOUNT]
        if bundle:
            args += ['--lua-services', str(bundle)]
        self.logs = tempfile.TemporaryFile()
        env = dict(os.environ)
        env.pop("ROTO_LUA_SERVICES", None)
        if environment_bundle is not None:
            env["ROTO_LUA_SERVICES"] = str(environment_bundle)
        self.process = subprocess.Popen(args, stdout=self.logs, stderr=self.logs, env=env)
        for _ in range(200):
            if self.process.poll() is not None:
                self.logs.seek(0)
                raise AssertionError(self.logs.read().decode())
            try:
                urllib.request.urlopen(self.url + '/roto-api/health', timeout=1).close()
                break
            except OSError:
                time.sleep(.05)
        else:
            self.close()
            raise AssertionError('Server failed to start')

    def close(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.logs.close()

    def call(self, service, op, data, status=200, error=None, account=ACCOUNT, region='us-east-1', unsigned=False):
        headers = {'Content-Type': 'application/x-amz-json-1.1', 'X-Amz-Target': f'{TARGETS[service]}.{op}'}
        if not unsigned:
            headers['Authorization'] = f'AWS4-HMAC-SHA256 Credential={account}/20261010/{region}/{service}/aws4_request, SignedHeaders=host, Signature=fake'
        req = urllib.request.Request(self.url, data=json.dumps(data).encode(), headers=headers)
        try:
            response = urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            value = json.load(response)
            assert response.status == status, (service, op, response.status, value)
            if error:
                assert value['__type'].split('#')[-1] == error, (op, value)
            if status == 200:
                self.seen.add((service, op))
            # Roto* control operations are not in any SDK model.
            if self.sdk and status == 200 and not op.startswith('Roto'):
                shape = self.models.get_service_model(service).operation_model(op).output_shape
                if shape:
                    validate_output(value, shape)
            return value

    def reset(self):
        req = urllib.request.Request(self.url + '/roto-api/reset', data=b'{}')
        urllib.request.urlopen(req, timeout=10).close()


def seed(s, name):
    """Create minimal state for HTTP bundle and persistence checks."""
    if name == 'cur':
        definition = {'ReportName': 'persist', 'TimeUnit': 'DAILY', 'Format': 'textORcsv', 'Compression': 'GZIP',
                      'AdditionalSchemaElements': ['RESOURCES'], 'S3Bucket': 'billing', 'S3Prefix': 'reports', 'S3Region': 'us-east-1'}
        s.call(name, 'PutReportDefinition', {'ReportDefinition': definition})
        return None
    if name == 'acm':
        return s.call(name, 'RequestCertificate', {'DomainName': 'example.com'})['CertificateArn']
    if name == 'budgets':
        s.call(name, 'CreateBudget', {'AccountId': ACCOUNT, 'Budget': {'BudgetName': 'persist', 'BudgetType': 'COST',
               'TimeUnit': 'MONTHLY', 'BudgetLimit': {'Amount': '600', 'Unit': 'USD'}}})
        return None
    if name == 'config':
        s.call(name, 'PutConfigurationRecorder', {'ConfigurationRecorder': {'name': 'default', 'roleARN': f'arn:aws:iam::{ACCOUNT}:role/config'}})
        return None
    op = s.call(name, 'CreateHttpNamespace', {'Name': 'persist.local'})['OperationId']
    nid = s.call(name, 'GetOperation', {'OperationId': op})['Operation']['Targets']['NAMESPACE']
    sid = s.call(name, 'CreateService', {'Name': 'api', 'NamespaceId': nid})['Service']['Id']
    s.call(name, 'RegisterInstance', {'ServiceId': sid, 'InstanceId': 'persist', 'Attributes': {'version': 'v1'}})
    return nid, sid


def sdk_check(s, name):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    client = boto3.client(name, endpoint_url=s.url, region_name='us-east-1',
                          aws_access_key_id=ACCOUNT, aws_secret_access_key='local-secret',
                          config=Config(retries={'max_attempts': 0}, connect_timeout=5, read_timeout=5, inject_host_prefix=False))
    if name == 'cur':
        definition = {'ReportName': 'sdk', 'TimeUnit': 'DAILY', 'Format': 'textORcsv', 'Compression': 'GZIP',
                      'AdditionalSchemaElements': ['RESOURCES'], 'S3Bucket': 'billing', 'S3Prefix': 'sdk', 'S3Region': 'us-east-1'}
        client.put_report_definition(ReportDefinition=definition)
        reports = [r for page in client.get_paginator('describe_report_definitions').paginate()
                   for r in page['ReportDefinitions']]
        assert any(r['ReportName'] == 'sdk' for r in reports)
        client.delete_report_definition(ReportName='sdk')
    elif name == 'acm':
        arn = client.import_certificate(Certificate=b'sdk-pem', PrivateKey=b'sdk-private', CertificateChain=b'sdk-chain')['CertificateArn']
        assert client.get_certificate(CertificateArn=arn)['Certificate'] == 'sdk-pem'
        cert = client.describe_certificate(CertificateArn=arn)['Certificate']
        assert isinstance(cert['CreatedAt'], datetime.datetime)
        assert any(c['CertificateArn'] == arn for page in client.get_paginator('list_certificates').paginate(PaginationConfig={'PageSize': 1})
                   for c in page['CertificateSummaryList'])
        client.delete_certificate(CertificateArn=arn)
    elif name == 'budgets':
        client.create_budget(AccountId=ACCOUNT, Budget={'BudgetName': 'sdk', 'TimeUnit': 'MONTHLY', 'BudgetType': 'COST', 'BudgetLimit': {'Amount': '1', 'Unit': 'USD'}})
        assert isinstance(client.describe_budget(AccountId=ACCOUNT, BudgetName='sdk')['Budget']['TimePeriod']['Start'], datetime.datetime)
        assert any(b['BudgetName'] == 'sdk' for page in client.get_paginator('describe_budgets').paginate(AccountId=ACCOUNT, PaginationConfig={'PageSize': 1}) for b in page['Budgets'])
        client.delete_budget(AccountId=ACCOUNT, BudgetName='sdk')
    elif name == 'config':
        client.put_config_rule(ConfigRule={'ConfigRuleName': 'sdk', 'Source': {'Owner': 'AWS', 'SourceIdentifier': 'S3_BUCKET_VERSIONING_ENABLED'}})
        assert any(r['ConfigRuleName'] == 'sdk' for page in client.get_paginator('describe_config_rules').paginate() for r in page['ConfigRules'])
        client.delete_config_rule(ConfigRuleName='sdk')
    else:
        oid = client.create_http_namespace(Name='sdk.local')['OperationId']
        nid = client.get_operation(OperationId=oid)['Operation']['Targets']['NAMESPACE']
        sid = client.create_service(Name='api', NamespaceId=nid, HealthCheckCustomConfig={'FailureThreshold': 1})['Service']['Id']
        client.register_instance(ServiceId=sid, InstanceId='sdk', Attributes={'version': 'v1'})
        assert client.discover_instances(NamespaceName='sdk.local', ServiceName='api')['Instances'][0]['InstanceId'] == 'sdk'
        client.update_instance_custom_health_status(ServiceId=sid, InstanceId='sdk', Status='UNHEALTHY')
        assert client.get_instances_health_status(ServiceId=sid)['Status'] == {'sdk': 'UNHEALTHY'}
        client.deregister_instance(ServiceId=sid, InstanceId='sdk')
        client.delete_service(Id=sid)
        client.delete_namespace(Id=nid)
    try:
        getattr(client, {'cur': 'delete_report_definition', 'acm': 'describe_certificate', 'budgets': 'describe_budget',
                         'config': 'delete_config_rule', 'servicediscovery': 'get_namespace'}[name])(**{
            'cur': {'ReportName': 'sdk-missing'}, 'acm': {'CertificateArn': f'arn:aws:acm:us-east-1:{ACCOUNT}:certificate/missing'},
            'budgets': {'AccountId': ACCOUNT, 'BudgetName': 'sdk-missing'}, 'config': {'ConfigRuleName': 'sdk-missing'},
            'servicediscovery': {'Id': 'ns-missing'},
        }[name])
        raise AssertionError('SDK failed to surface a modeled error')
    except ClientError as exc:
        expected = {'cur': 'ValidationException', 'acm': 'ResourceNotFoundException', 'budgets': 'NotFoundException',
                    'config': 'NoSuchConfigRuleException', 'servicediscovery': 'NamespaceNotFound'}[name]
        assert exc.response['Error']['Code'] == expected
    finally:
        client.close()


def persisted(s, name, fixture):
    if name == 'cur':
        assert s.call(name, 'DescribeReportDefinitions', {})['ReportDefinitions'][0]['ReportName'] == 'persist'
    elif name == 'acm':
        assert s.call(name, 'DescribeCertificate', {'CertificateArn': fixture})['Certificate']['DomainName'] == 'example.com'
    elif name == 'budgets':
        assert s.call(name, 'DescribeBudget', {'AccountId': ACCOUNT, 'BudgetName': 'persist'})['Budget']['BudgetLimit']['Amount'] == '600'
    elif name == 'config':
        assert s.call(name, 'DescribeConfigurationRecorderStatus', {})['ConfigurationRecordersStatus'][0]['recording'] is False
    else:
        _, sid = fixture
        assert s.call(name, 'GetInstance', {'ServiceId': sid, 'InstanceId': 'persist'})['Instance']['Attributes'] == {'version': 'v1'}


def empty(s, name):
    op, payload, key = {
        'cur': ('DescribeReportDefinitions', {}, 'ReportDefinitions'),
        'acm': ('ListCertificates', {}, 'CertificateSummaryList'),
        'budgets': ('DescribeBudgets', {'AccountId': ACCOUNT}, 'Budgets'),
        'config': ('DescribeConfigRules', {}, 'ConfigRules'),
        'servicediscovery': ('ListNamespaces', {}, 'Namespaces'),
    }[name]
    assert s.call(name, op, payload) == {key: []}
    unsigned_payload = dict(payload)
    assert s.call(name, op, unsigned_payload, unsigned=True) == {key: []}


def load_module(kind):
    import importlib.util
    spec = importlib.util.spec_from_file_location(kind, ROOT / f'tests/smoke-lua-{kind}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--service', choices=list(TARGETS) + ['opensearch', 'rds'], help='Run one service (default: all)')
    parser.add_argument('--sdk', action='store_true', help='Also validate output models and boto3 round trips (requires boto3)')
    args = parser.parse_args()
    external = {name: 'catalog' for name in ('ecr', 'ecs', 'glue', 'wafv2')}
    external.update({name: 'databases' for name in ('opensearch', 'rds')})
    for name in ([args.service] if args.service in external else external if args.service is None else []):
        load_module(external[name]).run(name, args.sdk)
    if args.service in external:
        return
    checks = ('cur', 'acm', 'budgets', 'config', 'servicediscovery')
    with tempfile.TemporaryDirectory(prefix='roto-bundle-') as packaged:
        archive = Path(packaged) / 'services.zip'
        subprocess.run([sys.executable, str(ROOT / 'scripts/package-lua-services.py'), '--output', str(archive)], check=True)
        with zipfile.ZipFile(archive) as z:
            assert z.namelist()[0] == 'manifest.json'
            assert z.getinfo('manifest.json').header_offset == 0
            assert all(not n.endswith('/manifest.json') for n in z.namelist())
        # A server with no configured bundle has no Lua service registrations.
        with tempfile.TemporaryDirectory(prefix='roto-no-bundle-') as directory:
            s = Server(directory)
            try:
                s.call('cur', 'DescribeReportDefinitions', {}, status=400, error='UnrecognizedClientException')
                assert not (Path(directory) / 'cur.db').exists()
            finally:
                s.close()
        for cli_bundle, env_bundle in [(None, archive), (ROOT / 'services', Path(packaged) / 'missing.zip')]:
            with tempfile.TemporaryDirectory(prefix='roto-env-bundle-') as directory:
                s = Server(directory, cli_bundle, environment_bundle=env_bundle)
                try:
                    empty(s, 'cur')
                finally:
                    s.close()
        for name in ([args.service] if args.service else checks):
            for bundle in (ROOT / 'services', archive):
                with tempfile.TemporaryDirectory(prefix=f'roto-{name}-') as directory:
                    s = Server(directory, bundle, args.sdk)
                    try:
                        empty(s, name)
                        fixture = seed(s, name)
                        if args.sdk:
                            sdk_check(s, name)
                    finally:
                        s.close()
                    s = Server(directory, bundle, args.sdk)
                    try:
                        persisted(s, name, fixture)
                        s.reset()
                        empty(s, name)
                        seed(s, name)
                    finally:
                        s.close()
                    s = Server(directory, bundle, args.sdk)
                    try:
                        s.reset()
                        empty(s, name)
                    finally:
                        s.close()
                print(f'{name}: HTTP/SDK contracts, isolation, persistence and reset passed ({"ZIP" if bundle == archive else "directory"})')


if __name__ == '__main__':
    main()
