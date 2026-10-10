#!/usr/bin/env python3
"""ECR/ECS/Glue configuration contracts, plus optional real boto3 round trips."""
import importlib.util
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

spec = importlib.util.spec_from_file_location('smoke', Path(__file__).with_name('smoke-lua-services.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)
ROOT, ACCOUNT, Server = smoke.ROOT, smoke.ACCOUNT, smoke.Server


def ecr(s):
    def call(op,data=None,**kw):return s.call('ecr',op,data or {},**kw)
    call('CreateRepository',{'repositoryName':'team/api','tags':[{'Key':'env','Value':'test'}]})
    repo=call('DescribeRepositories',{'repositoryNames':['team/api']})['repositories'][0]
    assert repo['registryId']==ACCOUNT and repo['imageScanningConfiguration']=={'scanOnPush':False}
    arn=repo['repositoryArn']
    call('CreateRepository',{'repositoryName':'team/api'},status=400,error='RepositoryAlreadyExistsException')
    call('CreateRepository',{'repositoryName':'Bad'},status=400,error='InvalidParameterException')
    call('CreateRepository',{'repositoryName':'kms','encryptionConfiguration':{'encryptionType':'KMS'}},status=501,error='NotImplemented')
    call('DescribeRepositories',{'registryId':'999999999999'},status=400,error='InvalidParameterException')
    call('DescribeRepositories',{'repositoryNames':['missing']},status=400,error='RepositoryNotFoundException')
    call('DescribeRepositories',{'nextToken':'bad'},status=400,error='InvalidParameterException')
    call('GetRepositoryPolicy',{'repositoryName':'team/api'},status=400,error='RepositoryPolicyNotFoundException')
    policy=json.dumps({'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':'*','Action':'ecr:BatchGetImage'}]})
    assert call('SetRepositoryPolicy',{'repositoryName':'team/api','policyText':policy})['policyText']==policy
    assert call('GetRepositoryPolicy',{'repositoryName':'team/api'})['policyText']==policy
    call('SetRepositoryPolicy',{'repositoryName':'team/api','policyText':'bad-json'},status=400,error='InvalidParameterException')
    assert call('GetRepositoryPolicy',{'repositoryName':'team/api'})['policyText']==policy
    assert call('DeleteRepositoryPolicy',{'repositoryName':'team/api'})['policyText']==policy
    call('DeleteRepositoryPolicy',{'repositoryName':'team/api'},status=400,error='RepositoryPolicyNotFoundException')
    life=json.dumps({'rules':[{'rulePriority':1,'description':'keep recent','selection':{'tagStatus':'any','countType':'imageCountMoreThan','countNumber':10},'action':{'type':'expire'}}]})
    call('GetLifecyclePolicy',{'repositoryName':'team/api'},status=400,error='LifecyclePolicyNotFoundException')
    call('PutLifecyclePolicy',{'repositoryName':'team/api','lifecyclePolicyText':life})
    assert call('GetLifecyclePolicy',{'repositoryName':'team/api'})['lifecyclePolicyText']==life
    assert 'lastEvaluatedAt' not in call('GetLifecyclePolicy',{'repositoryName':'team/api'})
    call('DeleteLifecyclePolicy',{'repositoryName':'team/api'})
    call('PutImageTagMutability',{'repositoryName':'team/api','imageTagMutability':'IMMUTABLE'})
    call('PutImageTagMutability',{'repositoryName':'team/api','imageTagMutability':'IMMUTABLE_WITH_EXCLUSION','imageTagMutabilityExclusionFilters':[{'filterType':'WILDCARD','filter':'dev-*'}]})
    call('PutImageTagMutability',{'repositoryName':'team/api','imageTagMutability':'invalid'},status=400,error='InvalidParameterException')
    call('PutImageScanningConfiguration',{'repositoryName':'team/api','imageScanningConfiguration':{'scanOnPush':True}})
    call('TagResource',{'resourceArn':arn,'tags':[{'Key':'env','Value':'updated'},{'Key':'drop','Value':'x'}]})
    call('UntagResource',{'resourceArn':arn,'tagKeys':['drop']})
    assert call('ListTagsForResource',{'resourceArn':arn})=={'tags':[{'Key':'env','Value':'updated'}]}
    call('ListTagsForResource',{'resourceArn':arn.replace(ACCOUNT,'999999999999')},status=400,error='InvalidParameterException')
    call('CreateRepository',{'repositoryName':'temp'})
    page=call('DescribeRepositories',{'maxResults':1})
    second=call('DescribeRepositories',{'maxResults':1,'nextToken':page['nextToken']})
    assert len(page['repositories'])==len(second['repositories'])==1
    assert page['repositories'][0]['repositoryName']!=second['repositories'][0]['repositoryName']
    call('DeleteRepository',{'repositoryName':'temp'})
    call('PutImage',{'repositoryName':'team/api','imageManifest':'{}'},status=501,error='NotImplemented')
    return arn


def ecs(s):
    def call(op,data=None,**kw):return s.call('ecs',op,data or {},**kw)
    c=call('CreateCluster',{'clusterName':'demo','tags':[{'key':'env','value':'test'}]})['cluster']
    arn=c['clusterArn']
    assert c['runningTasksCount']==c['pendingTasksCount']==0
    call('UpdateCluster',{'cluster':'demo','configuration':{'executeCommandConfiguration':{'logging':'NONE'}},'settings':[{'name':'containerInsights','value':'disabled'}]})
    assert call('DescribeClusters',{'clusters':['demo']})['clusters'][0]['settings'][0]['value']=='disabled'
    call('UpdateClusterSettings',{'cluster':arn,'settings':[{'name':'containerInsights','value':'enabled'}]})
    assert call('DescribeClusters',{'clusters':['demo','missing'],'include':['TAGS']})['failures']==[{'arn':'missing','reason':'MISSING'}]
    call('CreateCluster',{'clusterName':'temporary'})
    page=call('ListClusters',{'maxResults':1});assert page['nextToken']
    assert call('ListClusters',{'nextToken':page['nextToken']})['clusterArns']
    call('DeleteCluster',{'cluster':'temporary'})
    td={'family':'web','containerDefinitions':[{'name':'web','image':'demo:latest','environment':[{'name':'MODE','value':'test'}],'portMappings':[{'containerPort':8080}]}],'networkMode':'awsvpc','requiresCompatibilities':['FARGATE'],'cpu':'256','memory':'512','tags':[{'key':'env','value':'test'}]}
    d1=call('RegisterTaskDefinition',td)['taskDefinition'];d2=call('RegisterTaskDefinition',td)['taskDefinition']
    assert d1['revision']==1 and d2['revision']==2
    assert call('DescribeTaskDefinition',{'taskDefinition':'web'})['taskDefinition']['revision']==2
    assert call('DescribeTaskDefinition',{'taskDefinition':d1['taskDefinitionArn'],'include':['TAGS']})['tags']==[{'key':'env','value':'test'}]
    assert len(call('ListTaskDefinitions',{'familyPrefix':'we'})['taskDefinitionArns'])==2
    assert call('ListTaskDefinitionFamilies',{'status':'ACTIVE'})['families']==['web']
    for _ in range(12):call('RegisterTaskDefinition',{'family':'ordered','containerDefinitions':[{'name':'main','image':'demo'}]})
    ordered=call('ListTaskDefinitions',{'familyPrefix':'ordered','sort':'DESC'})['taskDefinitionArns']
    assert ordered[0].endswith(':12') and ordered[-1].endswith(':1')
    call('DeregisterTaskDefinition',{'taskDefinition':'web:1'})
    assert call('ListTaskDefinitions',{'status':'INACTIVE'})['taskDefinitionArns']==[d1['taskDefinitionArn']]
    assert call('DescribeTaskDefinition',{'taskDefinition':'web:1'})['taskDefinition']['status']=='INACTIVE'
    call('CreateService',{'cluster':'missing','serviceName':'api','taskDefinition':'web'},status=400,error='ClusterNotFoundException')
    call('CreateService',{'cluster':'demo','serviceName':'api','taskDefinition':'web:1'},status=400,error='ClientException')
    svc=call('CreateService',{'cluster':'demo','serviceName':'api','taskDefinition':'web','desiredCount':2,'launchType':'FARGATE','role':'service-role','networkConfiguration':{'awsvpcConfiguration':{'subnets':['subnet-demo'],'assignPublicIp':'DISABLED'}}})['service']
    assert svc['runningCount']==svc['pendingCount']==0 and svc['deployments']==[]
    assert svc['roleArn']==f'arn:aws:iam::{ACCOUNT}:role/service-role'
    assert call('DescribeClusters',{'clusters':['demo']})['clusters'][0]['activeServicesCount']==1
    assert call('ListServices',{'cluster':'demo','launchType':'FARGATE'})['serviceArns']==[svc['serviceArn']]
    assert call('DescribeServices',{'cluster':'demo','services':['api','missing']})['failures']==[{'arn':'missing','reason':'MISSING'}]
    call('DeleteCluster',{'cluster':'demo'},status=400,error='ClusterContainsServicesException')
    call('DeleteService',{'cluster':'demo','service':'api'},status=400,error='InvalidParameterException')
    call('UpdateService',{'cluster':'demo','service':'api','desiredCount':1})
    call('UpdateService',{'cluster':'demo','service':'api','forceNewDeployment':True},status=501,error='NotImplemented')
    for resource in [arn,d2['taskDefinitionArn'],svc['serviceArn']]:
        call('TagResource',{'resourceArn':resource,'tags':[{'key':'env','value':'updated'},{'key':'drop','value':'x'}]})
        call('UntagResource',{'resourceArn':resource,'tagKeys':['drop']})
        assert call('ListTagsForResource',{'resourceArn':resource})['tags']==[{'key':'env','value':'updated'}]
    call('ListTagsForResource',{'resourceArn':arn.replace(ACCOUNT,'999999999999')},status=400,error='InvalidParameterException')
    call('DeleteService',{'cluster':'demo','service':'api','force':True})
    assert call('DescribeServices',{'cluster':'demo','services':['api']})['services'][0]['status']=='INACTIVE'
    assert call('ListServices',{'cluster':'demo'})['serviceArns']==[]
    call('CreateService',{'cluster':'demo','serviceName':'api','taskDefinition':'web:2','desiredCount':3})
    call('RunTask',{'cluster':'demo','taskDefinition':'web'},status=501,error='NotImplemented')
    return arn


def glue(s):
    def call(op,data=None,**kw):return s.call('glue',op,data or {},**kw)
    call('CreateDatabase',{'DatabaseInput':{'Name':'Demo','Description':'catalog','Parameters':{'source':'test'}},'Tags':{'env':'test'}})
    assert call('GetDatabase',{'Name':'demo'})['Database']['Name']=='demo'
    call('CreateDatabase',{'DatabaseInput':{'Name':'demo'}},status=400,error='AlreadyExistsException')
    call('UpdateDatabase',{'Name':'demo','DatabaseInput':{'Name':'demo','Description':'updated'}})
    assert call('GetDatabase',{'Name':'demo'})['Database']['Description']=='updated'
    call('GetDatabases',{'CatalogId':'999999999999'},status=400,error='InvalidInputException')
    call('CreateDatabase',{'DatabaseInput':{'Name':'temp'}})
    pg=call('GetDatabases',{'MaxResults':1});assert call('GetDatabases',{'MaxResults':1,'NextToken':pg['NextToken']})['DatabaseList']
    sd={'Columns':[{'Name':'id','Type':'bigint'},{'Name':'payload','Type':'struct<x:string,y:array<int>>'}],'Location':'s3://demo/data','InputFormat':'parquet','OutputFormat':'parquet','SerdeInfo':{'SerializationLibrary':'demo','Parameters':{'k':'v'}}}
    ti={'Name':'events','StorageDescriptor':sd,'PartitionKeys':[{'Name':'region','Type':'string'},{'Name':'year','Type':'int'}],'Parameters':{'classification':'parquet'},'TableType':'EXTERNAL_TABLE'}
    call('CreateTable',{'DatabaseName':'demo','TableInput':ti})
    call('CreateTable',{'DatabaseName':'missing','TableInput':ti},status=400,error='EntityNotFoundException')
    call('CreateTable',{'DatabaseName':'demo','TableInput':ti},status=400,error='AlreadyExistsException')
    t=call('GetTable',{'DatabaseName':'demo','Name':'events'})['Table'];assert t['StorageDescriptor']==sd and t['VersionId']=='1'
    call('UpdateTable',{'DatabaseName':'demo','TableInput':dict(ti,Description='version two'),'VersionId':'1'})
    call('UpdateTable',{'DatabaseName':'demo','TableInput':dict(ti,Description='stale'),'VersionId':'1'},status=400,error='ConcurrentModificationException')
    assert call('GetTable',{'DatabaseName':'demo','Name':'events'})['Table']['Description']=='version two'
    assert 'Description' not in call('GetTableVersion',{'DatabaseName':'demo','TableName':'events','VersionId':'1'})['TableVersion']['Table']
    versions=call('GetTableVersions',{'DatabaseName':'demo','TableName':'events','MaxResults':1});assert versions['NextToken']
    assert call('GetTableVersions',{'DatabaseName':'demo','TableName':'events','NextToken':versions['NextToken']})['TableVersions'][0]['VersionId']=='2'
    call('UpdateTable',{'DatabaseName':'demo','TableInput':dict(ti,Description='persist'),'VersionId':'2'})
    call('DeleteTableVersion',{'DatabaseName':'demo','TableName':'events','VersionId':'1'})
    result=call('BatchDeleteTableVersion',{'DatabaseName':'demo','TableName':'events','VersionIds':['2','999','3']})
    assert [v['VersionId'] for v in result['Errors']]==['999','3']
    for _ in range(9):call('UpdateTable',{'DatabaseName':'demo','TableInput':{'Name':'events','Description':'persist'}})
    all_versions=call('GetTableVersions',{'DatabaseName':'demo','TableName':'events'})['TableVersions']
    assert [v['VersionId'] for v in all_versions]==[str(i) for i in range(3,13)]
    call('UpdateTable',{'DatabaseName':'demo','TableInput':{'Name':'events','Description':'persist'},'SkipArchive':True})
    call('GetTableVersion',{'DatabaseName':'demo','TableName':'events','VersionId':'12'},status=400,error='EntityNotFoundException')
    call('DeleteTableVersion',{'DatabaseName':'demo','TableName':'events','VersionId':'13'},status=400,error='InvalidInputException')
    call('CreateTable',{'DatabaseName':'demo','TableInput':{'Name':'temporary'}})
    assert len(call('GetTables',{'DatabaseName':'demo'})['TableList'])==2
    result=call('BatchDeleteTable',{'DatabaseName':'demo','TablesToDelete':['temporary','missing']})
    assert result['Errors'][0]['ErrorDetail']['ErrorCode']=='EntityNotFoundException'
    call('CreateTable',{'DatabaseName':'temp','TableInput':{'Name':'cascade','PartitionKeys':[{'Name':'p','Type':'string'}]}})
    call('CreatePartition',{'DatabaseName':'temp','TableName':'cascade','PartitionInput':{'Values':['a']}})
    call('DeleteTable',{'DatabaseName':'temp','Name':'cascade'})
    call('CreateTable',{'DatabaseName':'temp','TableInput':{'Name':'cascade','PartitionKeys':[{'Name':'p','Type':'string'}]}})
    assert call('GetPartitions',{'DatabaseName':'temp','TableName':'cascade'})['Partitions']==[]
    call('DeleteDatabase',{'Name':'temp'})
    base={'DatabaseName':'demo','TableName':'events'}
    call('CreatePartition',dict(base,PartitionInput={'Values':['eu','2025'],'StorageDescriptor':sd,'Parameters':{'batch':'one'}}))
    result=call('BatchCreatePartition',dict(base,PartitionInputList=[{'Values':['us','2026']},{'Values':['eu','2025']},{'Values':['bad']}]))
    assert [v['ErrorDetail']['ErrorCode'] for v in result['Errors']]==['AlreadyExistsException','InvalidInputException']
    assert len(call('GetPartitions',base)['Partitions'])==2
    assert call('GetPartitions',dict(base,Expression="region = 'eu' AND year >= 2025"))['Partitions'][0]['Values']==['eu','2025']
    assert call('GetPartitions',dict(base,Expression='year > 2025'))['Partitions'][0]['Values']==['us','2026']
    assert call('GetPartitions',dict(base,Expression="region = 'none'"))['Partitions']==[]
    call('GetPartitions',dict(base,Expression="region = 'eu' OR year = 2026"),status=400,error='InvalidInputException')
    call('GetPartitions',dict(base,Expression="unknown = 'x'"),status=400,error='InvalidInputException')
    selected=call('GetPartitions',dict(base,MaxResults=1));assert selected['NextToken']
    assert call('GetPartitions',dict(base,NextToken=selected['NextToken']))['Partitions']
    assert call('GetPartition',dict(base,PartitionValues=['eu','2025']))['Partition']['StorageDescriptor']==sd
    call('UpdatePartition',dict(base,PartitionValueList=['eu','2025'],PartitionInput={'Values':['eu','2026'],'Parameters':{'batch':'updated'}}))
    call('UpdatePartition',dict(base,PartitionValueList=['eu','2026'],PartitionInput={'Values':['us','2026']}),status=400,error='AlreadyExistsException')
    got=call('BatchGetPartition',dict(base,PartitionsToGet=[{'Values':['eu','2026']},{'Values':['none','2026']}]))
    assert len(got['Partitions'])==1 and got['UnprocessedKeys']==[{'Values':['none','2026']}]
    call('CreatePartition',dict(base,PartitionInput={'Values':['drop','2026']}))
    call('DeletePartition',dict(base,PartitionValues=['drop','2026']))
    result=call('BatchDeletePartition',dict(base,PartitionsToDelete=[{'Values':['us','2026']},{'Values':['none','2026']}]))
    assert result['Errors'][0]['ErrorDetail']['ErrorCode']=='EntityNotFoundException'
    # A handler that writes the first entry then fails the second must roll back all writes.
    call('BatchCreatePartition',dict(base,PartitionInputList=[{'Values':['rollback','2026']},{'Values':'invalid'}]),status=400,error='InvalidInputException')
    assert len(call('GetPartitions',base)['Partitions'])==1
    call('CreatePartition',dict(base,PartitionInput={'Values':["O'Reilly AND Co",'10']}))
    call('CreatePartition',dict(base,PartitionInput={'Values':['number','2']}))
    assert call('GetPartitions',dict(base,Expression="region = 'O''Reilly AND Co' AND year > 9"))['Partitions'][0]['Values']==["O'Reilly AND Co",'10']
    assert len(call('GetPartitions',dict(base,Expression='year > 9'))['Partitions'])==2
    call('DeletePartition',dict(base,PartitionValues=["O'Reilly AND Co",'10']))
    call('DeletePartition',dict(base,PartitionValues=['number','2']))
    call('UpdateTable',{'DatabaseName':'demo','TableInput':{'Name':'events','PartitionKeys':[]}},status=400,error='InvalidInputException')
    arn=f'arn:aws:glue:us-east-1:{ACCOUNT}:database/demo'
    call('TagResource',{'ResourceArn':arn,'TagsToAdd':{'env':'updated','drop':'x'}})
    call('UntagResource',{'ResourceArn':arn,'TagsToRemove':['drop']})
    assert call('GetTags',{'ResourceArn':arn})['Tags']=={'env':'updated'}
    call('GetTags',{'ResourceArn':arn.replace(ACCOUNT,'999999999999')},status=400,error='InvalidInputException')
    call('StartJobRun',{'JobName':'demo'},status=501,error='NotImplemented')
    return arn


def empty(s,name,**kw):
    op,key={'ecr':('DescribeRepositories','repositories'),'ecs':('ListClusters','clusterArns'),'glue':('GetDatabases','DatabaseList')}[name]
    assert s.call(name,op,{},**kw)=={key:[]}


def persisted(s,name,fixture):
    if name=='ecr':
        repo=s.call(name,'DescribeRepositories',{'repositoryNames':['team/api']})['repositories'][0]
        assert repo['imageTagMutability']=='IMMUTABLE_WITH_EXCLUSION' and repo['imageScanningConfiguration']['scanOnPush']
    elif name=='ecs':
        assert s.call(name,'DescribeTaskDefinition',{'taskDefinition':'web'})['taskDefinition']['revision']==2
        assert s.call(name,'DescribeServices',{'cluster':'demo','services':['api']})['services'][0]['desiredCount']==3
    else:
        assert s.call(name,'GetTable',{'DatabaseName':'demo','Name':'events'})['Table']['VersionId']=='13'
        assert s.call(name,'GetPartition',{'DatabaseName':'demo','TableName':'events','PartitionValues':['eu','2026']})['Partition']['Parameters']['batch']=='updated'


def sdk_check(s,name):
    import boto3
    from botocore.config import Config
    c=boto3.client(name,endpoint_url=s.url,region_name='us-east-1',aws_access_key_id='test',aws_secret_access_key='test',config=Config(retries={'max_attempts':0}))
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
    checks={'ecr':ecr,'ecs':ecs,'glue':glue}
    with tempfile.TemporaryDirectory(prefix='roto-catalog-bundle-') as packaged:
        archive=Path(packaged)/'services.zip'
        subprocess.run([sys.executable,str(ROOT/'scripts/package-lua-services.py'),'--output',str(archive)],check=True)
        for bundle in [ROOT/'services',archive]:
            with tempfile.TemporaryDirectory(prefix='roto-'+name+'-') as directory:
                s=Server(directory,bundle,sdk)
                try:
                    empty(s,name);empty(s,name,unsigned=True)
                    fixture=checks[name](s)
                    exported=set(re.findall(r'function ops\.(\w+)',(ROOT/'services'/name/'service.lua').read_text()))
                    covered={op for service,op in s.seen if service==name}
                    assert exported<=covered,(name,exported-covered)
                    empty(s,name,region='eu-west-1');empty(s,name,account='999999999999')
                    if sdk:sdk_check(s,name)
                finally:s.close()
                s=Server(directory,bundle,sdk)
                try:
                    persisted(s,name,fixture);s.reset();empty(s,name)
                    checks[name](s)
                finally:s.close()
                s=Server(directory,bundle,sdk)
                try:s.reset();empty(s,name)
                finally:s.close()
            print(f'{name}: {len(exported)} operations, HTTP, models, isolation, persistence/reset and rollback passed ({"ZIP" if bundle==archive else "directory"})')


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--service',choices=['ecr','ecs','glue'],required=True);p.add_argument('--sdk',action='store_true');a=p.parse_args();run(a.service,a.sdk)
