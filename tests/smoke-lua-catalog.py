#!/usr/bin/env python3
"""ECR/ECS/Glue/WAFv2 bundle persistence, routing and optional boto3 round trips."""
import importlib.util
import json
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

spec = importlib.util.spec_from_file_location('smoke', Path(__file__).with_name('smoke-lua-services.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)
ROOT, ACCOUNT, Server = smoke.ROOT, smoke.ACCOUNT, smoke.Server


def seed(s, name):
    """Create one fixture for directory/ZIP, persistence and reset checks."""
    def call(op, data):
        return s.call(name, op, data)
    if name == 'ecr':
        repo = call('CreateRepository', {'repositoryName': 'persist', 'imageTagMutability': 'IMMUTABLE',
                                        'imageScanningConfiguration': {'scanOnPush': True}})['repository']
        return repo['repositoryArn']
    if name == 'ecs':
        call('CreateCluster', {'clusterName': 'persist'})
        definition = call('RegisterTaskDefinition', {'family': 'persist', 'containerDefinitions': [{'name': 'main', 'image': 'demo'}]})['taskDefinition']
        call('CreateService', {'cluster': 'persist', 'serviceName': 'api', 'taskDefinition': definition['taskDefinitionArn'], 'desiredCount': 3})
        return definition['taskDefinitionArn']
    if name == 'glue':
        call('CreateDatabase', {'DatabaseInput': {'Name': 'persist'}})
        call('CreateTable', {'DatabaseName': 'persist', 'TableInput': {'Name': 'events', 'PartitionKeys': [{'Name': 'year', 'Type': 'int'}]}})
        call('CreatePartition', {'DatabaseName': 'persist', 'TableName': 'events', 'PartitionInput': {'Values': ['2026'], 'Parameters': {'batch': 'updated'}}})
        return None
    visibility = {'SampledRequestsEnabled': True, 'CloudWatchMetricsEnabled': True, 'MetricName': 'persist'}
    acl = call('CreateWebACL', {'Name': 'persist', 'Scope': 'REGIONAL', 'DefaultAction': {'Block': {}}, 'VisibilityConfig': visibility})['Summary']
    alb = 'arn:aws:elasticloadbalancing:us-east-1:' + ACCOUNT + ':loadbalancer/app/web/50dc6c495c0c9188'
    call('AssociateWebACL', {'WebACLArn': acl['ARN'], 'ResourceArn': alb})
    return acl['Id']


def empty(s,name,**kw):
    op,key={'ecr':('DescribeRepositories','repositories'),'ecs':('ListClusters','clusterArns'),'glue':('GetDatabases','DatabaseList'),'wafv2':('ListWebACLs','WebACLs')}[name]
    assert s.call(name,op,{'Scope':'REGIONAL'} if name=='wafv2' else {},**kw)=={key:[]}


def persisted(s,name,fixture):
    if name == 'ecr':
        repo = s.call(name, 'DescribeRepositories', {'repositoryNames': ['persist']})['repositories'][0]
        assert repo['imageTagMutability'] == 'IMMUTABLE' and repo['imageScanningConfiguration']['scanOnPush']
    elif name == 'ecs':
        assert s.call(name, 'DescribeTaskDefinition', {'taskDefinition': 'persist'})['taskDefinition']['taskDefinitionArn'] == fixture
        assert s.call(name, 'DescribeServices', {'cluster': 'persist', 'services': ['api']})['services'][0]['desiredCount'] == 3
    elif name == 'wafv2':
        acl = s.call(name, 'GetWebACL', {'Name': 'persist', 'Scope': 'REGIONAL', 'Id': fixture})['WebACL']
        assert acl['DefaultAction'] == {'Block': {}}
        alb = 'arn:aws:elasticloadbalancing:us-east-1:' + ACCOUNT + ':loadbalancer/app/web/50dc6c495c0c9188'
        assert s.call(name, 'GetWebACLForResource', {'ResourceArn': alb})['WebACL']['Id'] == fixture
    else:
        assert s.call(name, 'GetTable', {'DatabaseName': 'persist', 'Name': 'events'})['Table']['VersionId'] == '1'
        assert s.call(name, 'GetPartition', {'DatabaseName': 'persist', 'TableName': 'events', 'PartitionValues': ['2026']})['Partition']['Parameters']['batch'] == 'updated'


def sdk_check(s,name):
    import boto3
    from botocore.config import Config
    c=boto3.client(name,endpoint_url=s.url,region_name='us-east-1',aws_access_key_id='test',aws_secret_access_key='test',config=Config(retries={'max_attempts':0}))
    if name=='wafv2':
        vis={'SampledRequestsEnabled':True,'CloudWatchMetricsEnabled':True,'MetricName':'sdk'}
        ip=c.create_ip_set(Name='sdk',Scope='REGIONAL',IPAddressVersion='IPV4',Addresses=['192.0.2.0/24'])['Summary']
        acl=c.create_web_acl(Name='sdk',Scope='REGIONAL',DefaultAction={'Allow':{}},VisibilityConfig=vis,Rules=[{'Name':'r','Priority':0,'Statement':{'IPSetReferenceStatement':{'ARN':ip['ARN']}},'Action':{'Block':{}},'VisibilityConfig':vis}],Tags=[{'Key':'sdk','Value':'yes'}])['Summary']
        assert c.get_web_acl(Name='sdk',Scope='REGIONAL',Id=acl['Id'])['WebACL']['Rules'][0]['Name']=='r'
        assert c.list_tags_for_resource(ResourceARN=acl['ARN'])['TagInfoForResource']['TagList'][0]['Value']=='yes'
        assert [i['Name'] for i in c.list_ip_sets(Scope='REGIONAL')['IPSets']].count('sdk')==1
        try:c.delete_ip_set(Name='sdk',Scope='REGIONAL',Id=ip['Id'],LockToken=ip['LockToken']);raise AssertionError('referenced IP set was deleted')
        except c.exceptions.WAFAssociatedItemException:pass
        c.delete_web_acl(Name='sdk',Scope='REGIONAL',Id=acl['Id'],LockToken=acl['LockToken'])
        c.delete_ip_set(Name='sdk',Scope='REGIONAL',Id=ip['Id'],LockToken=ip['LockToken'])
        return
    if name=='ecr':
        repo=c.create_repository(repositoryName='sdk')['repository'];assert repo['createdAt'].year>=2026
        arn=repo['repositoryArn'];c.tag_resource(resourceArn=arn,tags=[{'Key':'sdk','Value':'yes'}]);assert c.list_tags_for_resource(resourceArn=arn)['tags'][0]['Value']=='yes'
        assert any(r['repositoryName']=='sdk' for page in c.get_paginator('describe_repositories').paginate(PaginationConfig={'PageSize':1}) for r in page['repositories'])
        try:c.get_repository_policy(repositoryName='sdk')
        except c.exceptions.RepositoryPolicyNotFoundException:pass
        else:raise AssertionError('Expected modeled error')
        c.delete_repository(repositoryName='sdk')
    elif name=='ecs':
        c.create_cluster(clusterName='sdk')
        td=c.register_task_definition(family='sdk',containerDefinitions=[{'name':'main','image':'demo'}])['taskDefinition'];assert td['registeredAt'].year>=2026
        c.create_service(cluster='sdk',serviceName='api',taskDefinition=td['taskDefinitionArn'],desiredCount=1)
        assert c.describe_services(cluster='sdk',services=['api'])['services'][0]['runningCount']==0
        assert any(arn.endswith('/sdk') for page in c.get_paginator('list_clusters').paginate(PaginationConfig={'PageSize':1}) for arn in page['clusterArns'])
        try:c.delete_cluster(cluster='sdk')
        except c.exceptions.ClusterContainsServicesException:pass
        else:raise AssertionError('Expected modeled error')
        c.delete_service(cluster='sdk',service='api',force=True);c.delete_cluster(cluster='sdk');c.deregister_task_definition(taskDefinition=td['taskDefinitionArn'])
    else:
        c.create_database(DatabaseInput={'Name':'sdk'})
        assert c.get_database(Name='sdk')['Database']['CreateTime'].year>=2026
        c.create_table(DatabaseName='sdk',TableInput={'Name':'table','PartitionKeys':[{'Name':'year','Type':'int'}],'StorageDescriptor':{'Columns':[{'Name':'id','Type':'string'}]}})
        c.batch_create_partition(DatabaseName='sdk',TableName='table',PartitionInputList=[{'Values':['2025']},{'Values':['2026']}])
        assert c.get_partitions(DatabaseName='sdk',TableName='table',Expression='year >= 2026')['Partitions'][0]['Values']==['2026']
        assert len([p for page in c.get_paginator('get_partitions').paginate(DatabaseName='sdk',TableName='table',PaginationConfig={'PageSize':1}) for p in page['Partitions']])==2
        try:c.get_table(DatabaseName='sdk',Name='missing')
        except c.exceptions.EntityNotFoundException:pass
        else:raise AssertionError('Expected modeled error')
        c.delete_database(Name='sdk')
    c.close()


def run(name,sdk=False):
    with tempfile.TemporaryDirectory(prefix='roto-catalog-bundle-') as packaged:
        archive=Path(packaged)/'services.zip'
        subprocess.run([sys.executable,str(ROOT/'scripts/package-lua-services.py'),'--output',str(archive)],check=True)
        for bundle in [ROOT/'services',archive]:
            with tempfile.TemporaryDirectory(prefix='roto-'+name+'-') as directory:
                s=Server(directory,bundle,sdk)
                try:
                    empty(s,name);empty(s,name,unsigned=True)
                    fixture=seed(s,name)
                    empty(s,name,region='eu-west-1');empty(s,name,account='999999999999')
                    if sdk:sdk_check(s,name)
                finally:s.close()
                s=Server(directory,bundle,sdk)
                try:
                    if name == 'ecr':
                        # Summary must see persisted Luau data before an ECR request loads it.
                        with urllib.request.urlopen(s.url + '/roto-api/summary', timeout=10) as response:
                            summary = json.load(response)
                        repositories = summary['services']['ecr']['collections'][f'{ACCOUNT}/us-east-1/repositories']
                        assert repositories['count'] == 1 and repositories['identifiers'] == ['persist']
                    persisted(s,name,fixture);s.reset();empty(s,name)
                    seed(s,name)
                finally:s.close()
                s=Server(directory,bundle,sdk)
                try:s.reset();empty(s,name)
                finally:s.close()
            print(f'{name}: HTTP, SDK models, isolation and persistence/reset passed ({"ZIP" if bundle==archive else "directory"})')


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--service',choices=['ecr','ecs','glue','wafv2'],required=True);p.add_argument('--sdk',action='store_true');a=p.parse_args();run(a.service,a.sdk)
