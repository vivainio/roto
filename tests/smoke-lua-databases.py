#!/usr/bin/env python3
"""OpenSearch/RDS HTTP contracts; directory/ZIP, persistence, reset, isolation, SDK."""
import importlib.util
import json
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

spec = importlib.util.spec_from_file_location('smoke', Path(__file__).with_name('smoke-lua-services.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)
ROOT, ACCOUNT = smoke.ROOT, smoke.ACCOUNT

class Client:
    def __init__(self, server, name, region='us-east-1', account=ACCOUNT, unsigned=False):
        self.server, self.name, self.region, self.account, self.unsigned = server,name,region,account,unsigned
        self.model=json.loads((ROOT/'services'/name/'model.json').read_text())
    def call(self, op, data=None, error=None):
        data=data or {}
        operation=self.model['operations'][op]
        headers={}
        if not self.unsigned:
            signing='es' if self.name=='opensearch' else 'rds'
            headers['Authorization']=f'AWS4-HMAC-SHA256 Credential={self.account}/20261010/{self.region}/{signing}/aws4_request, SignedHeaders=host, Signature=fake'
        if self.name=='opensearch':
            method=operation['http']['method'];path=operation['http']['requestUri'];body=dict(data);query={}
            shape=self.model['shapes'][operation['input']['shape']]
            for k,v in shape['members'].items():
                if k not in data:continue
                wire=v.get('locationName',k)
                if v.get('location')=='uri':path=path.replace('{'+wire+'}',urllib.parse.quote(str(body.pop(k)),safe=''))
                if v.get('location')=='querystring':query[wire]=body.pop(k)
            path+='?'+urllib.parse.urlencode(query)
            headers['Content-Type']='application/json';payload=json.dumps(body).encode() if method=='POST' else None
        else:
            method='POST';path='/';fields={'Action':op,'Version':'2014-10-31'}
            def flatten(ref,value,key):
                shape=self.model['shapes'][ref['shape']]
                if shape['type']=='structure':
                    for k,v in value.items():
                        member=shape['members'][k];wire=member.get('locationName',k)
                        flatten(member,v,key+'.'+wire if key else wire)
                elif shape['type']=='list':
                    item=shape['member'].get('locationName','member')
                    for n,v in enumerate(value,1):flatten(shape['member'],v,f'{key}.{item}.{n}')
                else:fields[key]=str(value).lower() if isinstance(value,bool) else str(value)
            flatten(operation['input'],data,'')
            payload=urllib.parse.urlencode(fields).encode();headers['Content-Type']='application/x-www-form-urlencoded'
        request=urllib.request.Request(self.server.url+path,data=payload,headers=headers,method=method)
        try:response=urllib.request.urlopen(request,timeout=10)
        except urllib.error.HTTPError as exc:response=exc
        with response:
            content=response.read()
            if self.name=='opensearch':
                assert content.startswith(b'{'), (op,response.status,content)
                value=json.loads(content);code=value.get('__type','').split('#')[-1]
                if response.status == 200 and getattr(self.server, 'validate_wire', False):
                    import botocore.session
                    shape=botocore.session.get_session().get_service_model(self.name).operation_model(op).output_shape
                    if shape:smoke.validate_output(value,shape)
            else:
                root=ET.fromstring(content)
                for node in root.iter():node.tag=node.tag.split('}')[-1]
                code=root.findtext('Error/Code')
                def decode(ref,node):
                    shape=self.model['shapes'][ref['shape']]
                    if shape['type']=='structure':
                        out={}
                        allowed={m.get('locationName', k) for k,m in shape['members'].items()}
                        assert all(child.tag in allowed for child in node), (op,[child.tag for child in node],allowed)
                        for k,m in shape['members'].items():
                            child=node.find(m.get('locationName',k))
                            if child is not None:out[k]=decode(m,child)
                        return out
                    if shape['type']=='list':return [decode(shape['member'],c) for c in node]
                    if shape['type']=='boolean':return node.text=='true'
                    if shape['type'] in ('integer','long'):return int(node.text)
                    return node.text or ''
                out=operation.get('output')
                value=decode(out,root.find(op+'Result')) if out and not code else {}
            if error:
                assert response.status>=400 and code==error,(op,response.status,code,content)
            else:assert response.status==200,(op,response.status,content)
            return value

def check(c):
    if c.name=='opensearch':
        assert c.call('ListDomainNames')=={'DomainNames':[]}
        d=c.call('CreateDomain',{'DomainName':'demo','TagList':[{'Key':'env','Value':'a&b'}]})['DomainStatus']
        assert 'Endpoint' not in d and 'Endpoints' not in d
        c.call('CreateDomain',{'DomainName':'demo'},error='ResourceAlreadyExistsException')
        c.call('CreateDomain',{'DomainName':'bad','VPCOptions':{'SubnetIds':['subnet-demo']}},error='NotImplemented')
        assert len(c.call('ListDomainNames')['DomainNames'])==1
        c.call('DescribeDomain',{'DomainName':'missing'},error='ResourceNotFoundException')
        c.call('DescribeDomains',{'DomainNames':['demo']})
        result=c.call('UpdateDomainConfig',{'DomainName':'demo','ClusterConfig':{'InstanceType':'m6g.large.search','InstanceCount':2}})
        assert result['DomainConfig']['ClusterConfig']['Status']['UpdateVersion']==2
        assert c.call('DescribeDomainConfig',{'DomainName':'demo'})==result
        arn=d['ARN'];c.call('AddTags',{'ARN':arn,'TagList':[{'Key':'env','Value':'updated'},{'Key':'drop','Value':'x'}]})
        c.call('RemoveTags',{'ARN':arn,'TagKeys':['drop']})
        assert c.call('ListTags',{'ARN':arn})['TagList']==[{'Key':'env','Value':'updated'}]
        c.call('AddTags',{'ARN':arn,'TagList':[{'Key':'rollback','Value':'x'},{'Key':'invalid'}]},error='ValidationException')
        assert len(c.call('ListTags',{'ARN':arn})['TagList'])==1
        c.call('CreateDomain',{'DomainName':'temporary'})
        assert c.call('DeleteDomain',{'DomainName':'temporary'})['DomainStatus']['Deleted']
        c.call('DescribeDomain',{'DomainName':'temporary'},error='ResourceNotFoundException')
        c.call('ListVersions',error='NotImplemented')
    else:
        assert c.call('DescribeDBInstances')=={'DBInstances':[]}
        c.call('CreateDBCluster',{'DBClusterIdentifier':'cluster','Engine':'aurora-postgresql','MasterUsername':'demo','MasterUserPassword':'fake-password'})
        d=c.call('CreateDBInstance',{'DBInstanceIdentifier':'demo','DBInstanceClass':'db.t3.micro','Engine':'aurora-postgresql','DBClusterIdentifier':'cluster','Tags':[{'Key':'env','Value':'a&b'}]})['DBInstance']
        assert 'Endpoint' not in d
        c.call('CreateDBInstance',{'DBInstanceIdentifier':'demo','DBInstanceClass':'db.t3.micro','Engine':'postgres'},error='DBInstanceAlreadyExists')
        c.call('DescribeDBInstances',{'DBInstanceIdentifier':'missing'},error='DBInstanceNotFound')
        c.call('ModifyDBInstance',{'DBInstanceIdentifier':'demo','DeletionProtection':True,'ApplyImmediately':True})
        c.call('DeleteDBInstance',{'DBInstanceIdentifier':'demo','SkipFinalSnapshot':True},error='InvalidParameterCombination')
        c.call('ModifyDBInstance',{'DBInstanceIdentifier':'demo','DeletionProtection':False})
        c.call('DeleteDBCluster',{'DBClusterIdentifier':'cluster','SkipFinalSnapshot':True},error='InvalidDBClusterStateFault')
        c.call('CreateDBSubnetGroup',{'DBSubnetGroupName':'subnets','DBSubnetGroupDescription':'demo','SubnetIds':['subnet-one','subnet-two']})
        c.call('ModifyDBSubnetGroup',{'DBSubnetGroupName':'subnets','DBSubnetGroupDescription':'updated','SubnetIds':['subnet-two']})
        assert len(c.call('DescribeDBSubnetGroups')['DBSubnetGroups'][0]['Subnets'])==1
        c.call('DeleteDBSubnetGroup',{'DBSubnetGroupName':'subnets'})
        c.call('CreateDBParameterGroup',{'DBParameterGroupName':'params','DBParameterGroupFamily':'postgres16','Description':'demo'})
        c.call('ModifyDBParameterGroup',{'DBParameterGroupName':'params','Parameters':[{'ParameterName':'work_mem','ParameterValue':'4096','ApplyMethod':'immediate'}]})
        assert c.call('DescribeDBParameters',{'DBParameterGroupName':'params'})['Parameters'][0]['ParameterValue']=='4096'
        c.call('ModifyDBParameterGroup',{'DBParameterGroupName':'params','Parameters':[{'ParameterName':'rollback','ParameterValue':'1','ApplyMethod':'immediate'},{'ParameterName':'invalid','ParameterValue':'2','ApplyMethod':'bad'}]},error='InvalidParameterValue')
        assert len(c.call('DescribeDBParameters',{'DBParameterGroupName':'params'})['Parameters'])==1
        c.call('ResetDBParameterGroup',{'DBParameterGroupName':'params','ResetAllParameters':True})
        assert c.call('DescribeDBParameters',{'DBParameterGroupName':'params'})['Parameters']==[]
        c.call('DescribeDBParameterGroups');c.call('DeleteDBParameterGroup',{'DBParameterGroupName':'params'})
        arn=d['DBInstanceArn'];c.call('AddTagsToResource',{'ResourceName':arn,'Tags':[{'Key':'env','Value':'updated'},{'Key':'drop','Value':'x'}]})
        c.call('RemoveTagsFromResource',{'ResourceName':arn,'TagKeys':['drop']})
        assert c.call('ListTagsForResource',{'ResourceName':arn})['TagList']==[{'Key':'env','Value':'updated'}]
        c.call('ModifyDBCluster',{'DBClusterIdentifier':'cluster','BackupRetentionPeriod':7})
        c.call('CreateDBInstance',{'DBInstanceIdentifier':'second','DBInstanceClass':'db.t3.micro','Engine':'postgres','MasterUsername':'demo','MasterUserPassword':'fake-password'})
        pages=c.call('DescribeDBInstances',{'MaxRecords':1});assert len(pages['DBInstances'])==1
        assert len(c.call('DescribeDBInstances',{'MaxRecords':1,'Marker':pages['Marker']})['DBInstances'])==1
        c.call('DeleteDBInstance',{'DBInstanceIdentifier':'second','SkipFinalSnapshot':True})
        c.call('DescribeDBSnapshots',error='NotImplemented')

def exists(c):
    if c.name=='opensearch':assert c.call('DescribeDomain',{'DomainName':'demo'})['DomainStatus']['ClusterConfig']['InstanceCount']==2
    else:
        assert c.call('DescribeDBInstances')['DBInstances'][0]['DBInstanceIdentifier']=='demo'
        assert len(c.call('DescribeDBClusters')['DBClusters'][0]['DBClusterMembers'])==1

def empty(c):
    op,key=('ListDomainNames','DomainNames') if c.name=='opensearch' else ('DescribeDBInstances','DBInstances')
    assert c.call(op)[key]==[]

def sdk_checks(s,name):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    c=boto3.client(name,endpoint_url=s.url,region_name='us-east-1',aws_access_key_id='test',aws_secret_access_key='test',config=Config(retries={'max_attempts':0}))
    if name=='opensearch':
        assert c.describe_domain(DomainName='demo')['DomainStatus']['DomainName']=='demo'
        assert c.describe_domain_config(DomainName='demo')['DomainConfig']['ClusterConfig']['Status']['CreationDate'].year>=2026
        arn=c.describe_domain(DomainName='demo')['DomainStatus']['ARN']
        assert c.list_tags(ARN=arn)['TagList'][0]['Value']=='updated'
        try:c.describe_domain(DomainName='missing')
        except c.exceptions.ResourceNotFoundException:pass
        else:raise AssertionError('Missing modeled error')
        c.create_domain(DomainName='sdk');c.delete_domain(DomainName='sdk')
    else:
        assert c.describe_db_instances(DBInstanceIdentifier='demo')['DBInstances'][0]['InstanceCreateTime'].year>=2026
        assert list(c.get_paginator('describe_db_instances').paginate())[0]['DBInstances'][0]['DBInstanceIdentifier']=='demo'
        try:c.describe_db_instances(DBInstanceIdentifier='missing')
        except c.exceptions.DBInstanceNotFoundFault:pass
        else:raise AssertionError('Missing modeled error')
        c.create_db_instance(DBInstanceIdentifier='sdk',DBInstanceClass='db.t3.micro',Engine='postgres',MasterUsername='demo',MasterUserPassword='fake-password')
        c.delete_db_instance(DBInstanceIdentifier='sdk',SkipFinalSnapshot=True)
    c.close()

def run(name,sdk=False):
    with tempfile.TemporaryDirectory(prefix='roto-databases-') as temp:
        temp=Path(temp);archive=temp/'bundle.zip'
        subprocess.run([sys.executable,str(ROOT/'scripts/package-lua-services.py'),'--output',str(archive)],check=True)
        for i,bundle in enumerate([ROOT/'services',archive]):
            store=temp/str(i)
            s=smoke.Server(store,bundle)
            try:
                s.validate_wire=sdk
                c=Client(s,name);check(c)
                empty(Client(s,name,region='eu-west-1'));empty(Client(s,name,account='999999999999'))
                exists(Client(s,name,unsigned=True))
                if sdk:sdk_checks(s,name)
                traces=json.load(urllib.request.urlopen(s.url+'/roto-api/trace'))
                assert any(row.get('operation') == ('CreateDomain' if name=='opensearch' else 'CreateDBInstance') for row in traces['entries']), traces
            finally:s.close()
            s=smoke.Server(store,bundle)
            try:exists(Client(s,name));s.reset();empty(Client(s,name))
            finally:s.close()
            # Repopulate, restart without loading the module, then reset its unopened DB.
            s=smoke.Server(store,bundle)
            try:check(Client(s,name))
            finally:s.close()
            s=smoke.Server(store,bundle)
            try:s.reset();empty(Client(s,name))
            finally:s.close()
    print(name+': directory/ZIP, HTTP, errors, rollback, isolation, persistence and reset passed')

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--service',choices=['opensearch','rds'],required=True);p.add_argument('--sdk',action='store_true');a=p.parse_args();run(a.service,a.sdk)
