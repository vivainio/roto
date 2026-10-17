#!/usr/bin/env python3
# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""HTTP contracts adapted from MiniStack's CUR, ACM, Budgets, Config and Cloud Map tests.

Uses only stdlib. Starts its own server; checks directory and ZIP bundles,
regional/account isolation, pagination, transaction rollback, restart and reset.
"""
import argparse
import base64
import datetime
import json
import os
import sys
import zipfile
import re
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
            if self.sdk and status == 200:
                shape = self.models.get_service_model(service).operation_model(op).output_shape
                if shape:
                    validate_output(value, shape)
            return value

    def reset(self):
        req = urllib.request.Request(self.url + '/roto-api/reset', data=b'{}')
        urllib.request.urlopen(req, timeout=10).close()


def cur(s):
    call = lambda op, data, **kw: s.call('cur', op, data, **kw)
    definition = {'ReportName': 'report', 'TimeUnit': 'DAILY', 'Format': 'textORcsv', 'Compression': 'GZIP',
                  'AdditionalSchemaElements': ['RESOURCES'], 'S3Bucket': 'billing', 'S3Prefix': 'reports',
                  'S3Region': 'us-east-1', 'AdditionalArtifacts': []}
    assert call('DescribeReportDefinitions', {}) == {'ReportDefinitions': []}
    call('PutReportDefinition', {'ReportDefinition': definition})
    call('PutReportDefinition', {'ReportDefinition': definition}, status=400, error='DuplicateReportNameException')
    call('TagResource', {'ReportName': 'report', 'Tags': [{'Key': 'env', 'Value': 'dev'}]})
    call('TagResource', {'ReportName': 'report', 'Tags': [{'Key': 'env', 'Value': 'prod'}]})
    call('TagResource', {'ReportName': 'report', 'Tags': [{'Key': 'new', 'Value': 'yes'}, {'Key': 'bad', 'Value': 1}]}, status=400, error='ValidationException')
    assert call('ListTagsForResource', {'ReportName': 'report'})['Tags'] == [{'Key': 'env', 'Value': 'prod'}]
    call('UntagResource', {'ReportName': 'report', 'TagKeys': ['env', 'missing']})
    assert call('ListTagsForResource', {'ReportName': 'report'})['Tags'] == []
    modified = dict(definition, ReportName='changed', S3Prefix='new')
    call('ModifyReportDefinition', {'ReportName': 'report', 'ReportDefinition': modified})
    assert call('DescribeReportDefinitions', {}, region='eu-west-1')['ReportDefinitions'][0]['ReportName'] == 'report'
    assert call('DescribeReportDefinitions', {}, account='999999999999')['ReportDefinitions'] == []
    for i in range(6):
        call('PutReportDefinition', {'ReportDefinition': dict(definition, ReportName=f'report-{i}')})
    page = call('DescribeReportDefinitions', {'MaxResults': 5})
    assert len(page['ReportDefinitions']) == 5
    assert len(call('DescribeReportDefinitions', {'NextToken': page['NextToken']})['ReportDefinitions']) == 2
    call('DescribeReportDefinitions', {'NextToken': 'bad'}, status=400, error='ValidationException')
    call('DeleteReportDefinition', {'ReportName': 'report-0'})
    call('DeleteReportDefinition', {'ReportName': 'missing'}, status=400, error='ValidationException')
    call('ListTagsForResource', {'ReportName': 'missing'}, status=400, error='ResourceNotFoundException')
    call('PutReportDefinition', {'ReportDefinition': []}, status=400, error='ValidationException')


def acm(s):
    call = lambda op, data, **kw: s.call('acm', op, data, **kw)
    assert call('ListCertificates', {})['CertificateSummaryList'] == []
    arn = call('RequestCertificate', {'DomainName': 'example.com', 'SubjectAlternativeNames': ['*.example.com']})['CertificateArn']
    cert = call('DescribeCertificate', {'CertificateArn': arn})['Certificate']
    assert cert['Status'] == 'ISSUED'
    records = [v['ResourceRecord'] for v in cert['DomainValidationOptions']]
    assert records[0] == records[1]
    call('AddTagsToCertificate', {'CertificateArn': arn, 'Tags': [{'Key': 'env', 'Value': 'dev'}]})
    call('RemoveTagsFromCertificate', {'CertificateArn': arn, 'Tags': [{'Key': 'env'}]})
    assert call('ListTagsForCertificate', {'CertificateArn': arn})['Tags'] == []
    call('UpdateCertificateOptions', {'CertificateArn': arn, 'Options': {'CertificateTransparencyLoggingPreference': 'DISABLED'}})
    assert call('DescribeCertificate', {'CertificateArn': arn})['Certificate']['Options']['CertificateTransparencyLoggingPreference'] == 'DISABLED'
    call('DescribeCertificate', {'CertificateArn': arn}, region='eu-west-1', status=400, error='ResourceNotFoundException')
    call('DescribeCertificate', {'CertificateArn': arn}, account='999999999999', status=400, error='ResourceNotFoundException')
    call('RequestCertificate', {'DomainName': 'other.example.com'})
    page = call('ListCertificates', {'MaxItems': 1})
    assert len(page['CertificateSummaryList']) == 1 and 'NextToken' in page
    assert 'NextToken' not in call('ListCertificates', {'MaxItems': 1, 'NextToken': page['NextToken']})
    pem, chain = '-----BEGIN CERTIFICATE-----\nhello\n-----END CERTIFICATE-----\n', 'chain-data'
    payload = {'Certificate': base64.b64encode(pem.encode()).decode(),
               'CertificateChain': base64.b64encode(chain.encode()).decode(),
               'PrivateKey': base64.b64encode(b'private-do-not-store').decode()}
    imported = call('ImportCertificate', payload)['CertificateArn']
    assert call('GetCertificate', {'CertificateArn': imported}) == {'Certificate': pem, 'CertificateChain': chain}
    assert call('DescribeCertificate', {'CertificateArn': imported})['Certificate']['DomainName'].endswith('.invalid')
    payload['CertificateArn'] = imported
    payload['Certificate'] = base64.b64encode(b'replacement').decode()
    assert call('ImportCertificate', payload)['CertificateArn'] == imported
    assert call('GetCertificate', {'CertificateArn': imported})['Certificate'] == 'replacement'
    payload['CertificateArn'] = arn.replace(ACCOUNT, '999999999999')
    call('ImportCertificate', payload, status=400, error='ResourceNotFoundException')
    call('DeleteCertificate', {'CertificateArn': imported})
    call('GetCertificate', {'CertificateArn': imported}, status=400, error='ResourceNotFoundException')
    call('RenewCertificate', {'CertificateArn': arn}, status=501, error='NotImplemented')
    return arn


def budgets(s):
    call = lambda op, data, **kw: s.call('budgets', op, data, **kw)
    scope = {'AccountId': ACCOUNT, 'BudgetName': 'budget'}
    budget = {'BudgetName': 'budget', 'BudgetType': 'COST', 'TimeUnit': 'MONTHLY', 'BudgetLimit': {'Amount': '500', 'Unit': 'USD'},
              'FilterExpression': {'Dimensions': {'Key': 'SERVICE', 'Values': ['Amazon S3']}}, 'Metrics': ['UnblendedCost']}
    notification = {'NotificationType': 'ACTUAL', 'ComparisonOperator': 'GREATER_THAN', 'Threshold': 90}
    sub = {'SubscriptionType': 'EMAIL', 'Address': 'one@example.com'}
    call('CreateBudget', {'AccountId': ACCOUNT, 'Budget': budget, 'NotificationsWithSubscribers': [{'Notification': notification, 'Subscribers': [sub]}]})
    call('CreateBudget', {'AccountId': ACCOUNT, 'Budget': budget}, status=400, error='DuplicateRecordException')
    value = call('DescribeBudget', scope)['Budget']
    assert 'FilterExpression' not in value and 'Metrics' not in value
    assert 'FilterExpression' in call('DescribeBudget', dict(scope, ShowFilterExpression=True))['Budget']
    assert value['CalculatedSpend'] == {'ActualSpend': {'Amount': '0', 'Unit': 'USD'}}
    updated = datetime.datetime.fromtimestamp(value['LastUpdatedTime'], datetime.timezone.utc)
    assert value['TimePeriod']['Start'] == updated.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
    assert value['TimePeriod']['End'] == datetime.datetime(2087, 6, 15, tzinfo=datetime.timezone.utc).timestamp()
    call('UpdateBudget', {'AccountId': ACCOUNT, 'NewBudget': dict(budget, BudgetLimit={'Amount': '600', 'Unit': 'USD'})})
    assert call('DescribeBudget', scope, region='eu-west-1')['Budget']['BudgetLimit']['Amount'] == '600'
    call('DescribeBudget', dict(scope, AccountId='999999999999'), account='999999999999', status=400, error='NotFoundException')
    second = dict(notification, Threshold=50, ThresholdType='PERCENTAGE')
    call('CreateNotification', dict(scope, Notification=second, Subscribers=[sub]))
    call('CreateNotification', dict(scope, Notification=notification, Subscribers=[sub]), status=400, error='DuplicateRecordException')
    new = dict(second, Threshold=60)
    call('UpdateNotification', dict(scope, OldNotification=second, NewNotification=new))
    call('UpdateNotification', dict(scope, OldNotification=new, NewNotification=notification), status=400, error='DuplicateRecordException')
    assert len(call('DescribeNotificationsForBudget', scope)['Notifications']) == 2
    page = call('DescribeNotificationsForBudget', dict(scope, MaxResults=1))
    assert 'NextToken' in page
    assert len(call('DescribeNotificationsForBudget', dict(scope, MaxResults=1, NextToken=page['NextToken']))['Notifications']) == 1
    call('DeleteNotification', dict(scope, Notification=new))
    normalized = call('DescribeNotificationsForBudget', scope)['Notifications'][0]
    assert normalized['ThresholdType'] == 'PERCENTAGE'
    other = dict(sub, Address='two@example.com')
    call('CreateSubscriber', dict(scope, Notification=notification, Subscriber=other))
    call('CreateSubscriber', dict(scope, Notification=notification, Subscriber=other), status=400, error='DuplicateRecordException')
    renamed = dict(other, Address='three@example.com')
    call('UpdateSubscriber', dict(scope, Notification=notification, OldSubscriber=other, NewSubscriber=renamed))
    subs = call('DescribeSubscribersForNotification', dict(scope, Notification=notification))['Subscribers']
    assert subs == [sub, renamed]
    call('DeleteSubscriber', dict(scope, Notification=notification, Subscriber=renamed))
    call('DeleteSubscriber', dict(scope, Notification=notification, Subscriber=sub))
    assert call('DescribeNotificationsForBudget', scope)['Notifications'] == []
    resource = {'ResourceARN': f'arn:aws:budgets::{ACCOUNT}:budget/budget'}
    call('TagResource', dict(resource, ResourceTags=[{'Key': 'env', 'Value': 'dev'}]))
    call('TagResource', dict(resource, ResourceTags=[{'Key': 'env', 'Value': 'prod'}]))
    assert call('ListTagsForResource', resource)['ResourceTags'] == [{'Key': 'env', 'Value': 'prod'}]
    call('UntagResource', dict(resource, ResourceTagKeys=['env']))
    assert call('ListTagsForResource', resource)['ResourceTags'] == []
    call('TagResource', dict(resource, ResourceARN=resource['ResourceARN'].replace(ACCOUNT, '999999999999'), ResourceTags=[]), status=400, error='NotFoundException')
    # Inline validation must leave no partial budget or notifications behind.
    call('CreateBudget', {'AccountId': ACCOUNT, 'Budget': dict(budget, BudgetName='rollback'),
                         'NotificationsWithSubscribers': [{'Notification': notification, 'Subscribers': [sub]},
                                                          {'Notification': notification, 'Subscribers': []}]}, status=400, error='InvalidParameterException')
    call('DescribeBudget', dict(scope, BudgetName='rollback'), status=400, error='NotFoundException')
    call('CreateBudget', {'AccountId': ACCOUNT, 'Budget': dict(budget, BudgetName='delete')})
    call('DeleteBudget', dict(scope, BudgetName='delete'))
    assert len(call('DescribeBudgets', {'AccountId': ACCOUNT})['Budgets']) == 1


def config(s):
    call = lambda op, data, **kw: s.call('config', op, data, **kw)
    rule = {'ConfigRuleName': 'versioning', 'Source': {'Owner': 'AWS', 'SourceIdentifier': 'S3_BUCKET_VERSIONING_ENABLED'}}
    call('PutConfigRule', {'ConfigRule': rule})
    original = call('DescribeConfigRules', {})['ConfigRules'][0]
    call('PutConfigRule', {'ConfigRule': dict(rule, Description='updated')})
    assert call('DescribeConfigRules', {'ConfigRuleNames': ['versioning']})['ConfigRules'][0]['ConfigRuleId'] == original['ConfigRuleId']
    call('DescribeConfigRules', {'ConfigRuleNames': ['missing']}, status=400, error='NoSuchConfigRuleException')
    assert call('DescribeConfigRules', {}, region='eu-west-1')['ConfigRules'] == []
    assert call('DescribeConfigRules', {}, account='999999999999')['ConfigRules'] == []
    recorder = {'name': 'default', 'roleARN': f'arn:aws:iam::{ACCOUNT}:role/config'}
    call('PutConfigurationRecorder', {'ConfigurationRecorder': recorder})
    assert call('DescribeConfigurationRecorders', {})['ConfigurationRecorders'] == [recorder]
    call('StartConfigurationRecorder', {'ConfigurationRecorderName': 'default'})
    assert call('DescribeConfigurationRecorderStatus', {})['ConfigurationRecordersStatus'][0]['recording'] is True
    call('StopConfigurationRecorder', {'ConfigurationRecorderName': 'default'})
    assert call('DescribeConfigurationRecorderStatus', {})['ConfigurationRecordersStatus'][0]['recording'] is False
    call('StartConfigurationRecorder', {'ConfigurationRecorderName': 'missing'}, status=400, error='NoSuchConfigurationRecorderException')
    call('PutDeliveryChannel', {'DeliveryChannel': {'name': 'default', 's3BucketName': 'config-data'}})
    assert call('DescribeDeliveryChannels', {})['DeliveryChannels'][0]['s3BucketName'] == 'config-data'
    call('DescribeDeliveryChannels', {'DeliveryChannelNames': ['missing']}, status=400, error='NoSuchDeliveryChannelException')
    call('PutConfigRule', {'ConfigRule': dict(rule, ConfigRuleName='delete')})
    call('DeleteConfigRule', {'ConfigRuleName': 'delete'})
    call('DeleteConfigRule', {'ConfigRuleName': 'missing'}, status=400, error='NoSuchConfigRuleException')
    assert call('DescribeDeliveryChannelStatus', {}) == {'DeliveryChannelsStatus': [{'name': 'default'}]}
    assert call('DescribeComplianceByConfigRule', {})['ComplianceByConfigRules'][0]['Compliance']['ComplianceType'] == 'INSUFFICIENT_DATA'
    assert call('GetComplianceDetailsByConfigRule', {'ConfigRuleName': 'versioning'}) == {'EvaluationResults': []}
    assert call('DescribeConfigRuleEvaluationStatus', {})['ConfigRulesEvaluationStatus'][0]['FirstEvaluationStarted'] is False
    call('StartConfigRulesEvaluation', {'ConfigRuleNames': ['versioning']}, status=501, error='NotImplemented')


def cloudmap(s):
    call = lambda op, data, **kw: s.call('servicediscovery', op, data, **kw)
    op = call('CreateHttpNamespace', {'Name': 'apps.local', 'Tags': [{'Key': 'env', 'Value': 'dev'}]})['OperationId']
    operation = call('GetOperation', {'OperationId': op})['Operation']
    assert operation['Status'] == 'SUCCESS'
    nid = operation['Targets']['NAMESPACE']
    assert call('GetNamespace', {'Id': nid})['Namespace']['Properties'] == {'HttpProperties': {'HttpName': 'apps.local'}}
    call('CreateHttpNamespace', {'Name': 'apps.local'}, status=409, error='NamespaceAlreadyExists')
    call('GetNamespace', {'Id': nid}, region='eu-west-1', status=404, error='NamespaceNotFound')
    call('GetNamespace', {'Id': nid}, account='999999999999', status=404, error='NamespaceNotFound')
    call('UpdateHttpNamespace', {'Id': nid, 'Namespace': {'Description': 'updated'}})
    assert call('GetNamespace', {'Id': nid})['Namespace']['Description'] == 'updated'
    service = call('CreateService', {'Name': 'api', 'NamespaceId': nid, 'HealthCheckCustomConfig': {'FailureThreshold': 1}})['Service']
    sid = service['Id']
    assert call('GetNamespace', {'Id': nid})['Namespace']['ServiceCount'] == 1
    call('DeleteNamespace', {'Id': nid}, status=400, error='ResourceInUse')
    call('UpdateService', {'Id': sid, 'Service': {'Description': 'updated'}})
    assert call('GetService', {'Id': sid})['Service']['Description'] == 'updated'
    call('RegisterInstance', {'ServiceId': sid, 'InstanceId': 'one', 'Attributes': {'version': 'v1'}})
    call('RegisterInstance', {'ServiceId': sid, 'InstanceId': 'two', 'Attributes': {'version': 'v2'}})
    assert call('GetService', {'Id': sid})['Service']['InstanceCount'] == 2
    assert call('GetInstance', {'ServiceId': sid, 'InstanceId': 'one'}) == {'Instance': {'Id': 'one', 'Attributes': {'version': 'v1'}}}
    query = {'NamespaceName': 'apps.local', 'ServiceName': 'api'}
    assert len(call('DiscoverInstances', query)['Instances']) == 2
    assert call('DiscoverInstances', dict(query, QueryParameters={'version': 'v1'}))['Instances'][0]['InstanceId'] == 'one'
    revision = call('DiscoverInstancesRevision', query)['InstancesRevision']
    call('UpdateInstanceCustomHealthStatus', {'ServiceId': sid, 'InstanceId': 'one', 'Status': 'UNHEALTHY'})
    assert call('DiscoverInstancesRevision', query)['InstancesRevision'] > revision
    assert len(call('DiscoverInstances', query)['Instances']) == 1
    assert len(call('DiscoverInstances', dict(query, HealthStatus='ALL'))['Instances']) == 2
    assert call('DiscoverInstances', dict(query, HealthStatus='UNHEALTHY'))['Instances'][0]['InstanceId'] == 'one'
    assert call('GetInstancesHealthStatus', {'ServiceId': sid, 'Instances': ['one', 'missing']})['Status'] == {'one': 'UNHEALTHY', 'missing': 'UNKNOWN'}
    call('UpdateInstanceCustomHealthStatus', {'ServiceId': sid, 'InstanceId': 'one', 'Status': 'bad'}, status=400, error='InvalidInput')
    call('UpdateServiceAttributes', {'ServiceId': sid, 'Attributes': {'release': 'v1', 'team': 'platform'}})
    call('DeleteServiceAttributes', {'ServiceId': sid, 'Attributes': ['team']})
    assert call('GetServiceAttributes', {'ServiceId': sid})['ServiceAttributes']['Attributes'] == {'release': 'v1'}
    resource = {'ResourceARN': service['Arn']}
    call('TagResource', dict(resource, Tags=[{'Key': 'env', 'Value': 'dev'}]))
    assert call('ListTagsForResource', resource)['Tags'] == [{'Key': 'env', 'Value': 'dev'}]
    call('UntagResource', dict(resource, TagKeys=['env']))
    assert call('ListTagsForResource', resource)['Tags'] == []
    assert len(call('ListServices', {'Filters': [{'Name': 'NAMESPACE_ID', 'Values': [nid]}]})['Services']) == 1
    page = call('ListInstances', {'ServiceId': sid, 'MaxResults': 1})
    assert len(page['Instances']) == 1 and 'NextToken' in page
    assert 'NextToken' not in call('ListInstances', {'ServiceId': sid, 'MaxResults': 1, 'NextToken': page['NextToken']})
    assert call('ListNamespaces', {'Filters': [{'Name': 'TYPE', 'Values': ['DNS_PUBLIC']}]})['Namespaces'] == []
    page = call('ListOperations', {'MaxResults': 1, 'Filters': [{'Name': 'STATUS', 'Values': ['SUCCESS']}]})
    assert len(page['Operations']) == 1 and 'NextToken' in page
    assert set(page['Operations'][0]) == {'Id', 'Status'}
    call('DeleteService', {'Id': sid}, status=400, error='ResourceInUse')
    for iid in ['one', 'two']:
        call('DeregisterInstance', {'ServiceId': sid, 'InstanceId': iid})
    call('DeleteService', {'Id': sid})
    call('DeleteNamespace', {'Id': nid})
    call('CreatePrivateDnsNamespace', {'Name': 'dns.local', 'Vpc': 'vpc-demo'}, status=501, error='NotImplemented')
    # Keep a namespace and registration for restart checks.
    op = call('CreateHttpNamespace', {'Name': 'persist.local'})['OperationId']
    nid = call('GetOperation', {'OperationId': op})['Operation']['Targets']['NAMESPACE']
    sid = call('CreateService', {'Name': 'api', 'NamespaceId': nid})['Service']['Id']
    call('RegisterInstance', {'ServiceId': sid, 'InstanceId': 'persist', 'Attributes': {'version': 'v1'}})
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
        assert len(s.call(name, 'DescribeReportDefinitions', {})['ReportDefinitions']) == 5
    elif name == 'acm':
        assert s.call(name, 'DescribeCertificate', {'CertificateArn': fixture})['Certificate']['DomainName'] == 'example.com'
    elif name == 'budgets':
        assert s.call(name, 'DescribeBudget', {'AccountId': ACCOUNT, 'BudgetName': 'budget'})['Budget']['BudgetLimit']['Amount'] == '600'
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
    checks = {'cur': cur, 'acm': acm, 'budgets': budgets, 'config': config, 'servicediscovery': cloudmap}
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
                        fixture = checks[name](s)
                        exported = set(re.findall(r'function ops\.(\w+)', (ROOT / 'services' / name / 'service.luau').read_text()))
                        covered = {op for service, op in s.seen if service == name}
                        assert exported <= covered, (name, 'Operations missing successful coverage', exported - covered)
                        if args.sdk:
                            sdk_check(s, name)
                    finally:
                        s.close()
                    s = Server(directory, bundle, args.sdk)
                    try:
                        persisted(s, name, fixture)
                        s.reset()
                        empty(s, name)
                        checks[name](s)
                    finally:
                        s.close()
                    s = Server(directory, bundle, args.sdk)
                    try:
                        s.reset()
                        empty(s, name)
                    finally:
                        s.close()
                print(f'{name}: {len(exported)} operations, HTTP contracts, isolation, persistence and reset passed ({"ZIP" if bundle == archive else "directory"})')


if __name__ == '__main__':
    main()
