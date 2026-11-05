#!/usr/bin/env python3
"""Regenerate bundled wire descriptors from installed botocore (no service logic).

Each entry is (botocore service, bundle directory, operations or None for all). Descriptors keep
only what the wire codecs need: routes, input/output references and the shapes they reach.
`EXTRA` adds Roto-only control operations (``Roto*``) that let a fixture script set state which
real AWS never exposes, such as a target's health. They are reachable through ``roto.wire``.
"""
import json
from pathlib import Path
import botocore.session

root = Path(__file__).resolve().parents[1] / 'services'

EC2_OPERATIONS = [
    'DescribeInstances', 'DescribeInstanceStatus', 'StartInstances', 'StopInstances',
    'TerminateInstances', 'RebootInstances', 'RunInstances', 'DescribeSecurityGroups',
    'CreateSecurityGroup', 'DeleteSecurityGroup', 'CreateTags', 'DeleteTags', 'DescribeTags',
]

SERVICES = [
    ('opensearch', 'opensearch', None),
    ('rds', 'rds', None),
    ('elbv2', 'elbv2', None),
    ('elb', 'elb', None),
    ('autoscaling', 'autoscaling', None),
    ('ec2', 'ec2', EC2_OPERATIONS),
]

STRING = {'type': 'string'}


def structure(members, required=()):
    shape = {'type': 'structure', 'members': members}
    if required:
        shape['required'] = list(required)
    return shape


def extra_operation(name, input_members, required=(), wrapper=True):
    """A Query operation with no output members, plus the shapes it needs."""
    shapes = {
        'RotoString': STRING,
        f'{name}Input': structure(input_members, required),
        f'{name}Output': structure({}),
    }
    if wrapper:
        shapes[f'{name}Output']['resultWrapper'] = f'{name}Result'
    operation = {
        'http': {'method': 'POST', 'requestUri': '/'},
        'input': {'shape': f'{name}Input'},
        'output': {'shape': f'{name}Output'},
    }
    return name, operation, shapes


EXTRA = {
    'elbv2': [extra_operation('RotoSetTargetHealth', {
        'TargetGroupArn': {'shape': 'TargetGroupArn'},
        'Targets': {'shape': 'TargetDescriptions'},
        'State': {'shape': 'RotoString'},
        'Reason': {'shape': 'RotoString'},
        'Description': {'shape': 'RotoString'},
    }, ['TargetGroupArn', 'State'])],
    'elb': [extra_operation('RotoSetInstanceHealth', {
        'LoadBalancerName': {'shape': 'AccessPointName'},
        'Instances': {'shape': 'Instances'},
        'State': {'shape': 'RotoString'},
        'ReasonCode': {'shape': 'RotoString'},
        'Description': {'shape': 'RotoString'},
    }, ['LoadBalancerName', 'State'])],
    'ec2': [extra_operation('RotoSetInstanceState', {
        'InstanceIds': {'shape': 'InstanceIdStringList', 'locationName': 'InstanceId'},
        'State': {'shape': 'RotoString'},
    }, ['State'], wrapper=False)],
}

session = botocore.session.get_session()
for name, directory, operations_filter in SERVICES:
    model = session.get_service_data(name)
    shapes = {}

    def visit(ref):
        if not ref or 'shape' not in ref:
            return
        n = ref['shape']
        if n in shapes:
            return
        shape = {k: v for k, v in model['shapes'][n].items() if k != 'documentation'}
        for k in ['members', 'member', 'key', 'value']:
            if k in shape:
                if k == 'members':
                    shape[k] = {a: {b: c for b, c in v.items() if b != 'documentation'}
                                for a, v in shape[k].items()}
                else:
                    shape[k] = {a: b for a, b in shape[k].items() if a != 'documentation'}
        shapes[n] = shape
        for r in shape.get('members', {}).values():
            visit(r)
        for k in ['member', 'key', 'value']:
            visit(shape.get(k))

    operations = {}
    for n, o in model['operations'].items():
        if operations_filter is not None and n not in operations_filter:
            continue
        operations[n] = {k: v for k, v in o.items() if k in ['http', 'input', 'output']}
        visit(o.get('input'))
        visit(o.get('output'))
    for extra_name, operation, extra_shapes in EXTRA.get(directory, []):
        operations[extra_name] = operation
        for shape_name, shape in extra_shapes.items():
            shapes[shape_name] = shape
        for shape in extra_shapes.values():
            for member in shape.get('members', {}).values():
                visit(member)
    # Keep references to shapes the extras rely on even when no real operation uses them.
    missing = [n for n in operations_filter or [] if n not in model['operations']]
    assert not missing, missing
    out = {'metadata': model['metadata'], 'operations': operations, 'shapes': shapes}
    (root / directory).mkdir(exist_ok=True)
    path = root / directory / 'model.json'
    path.write_text(json.dumps(out, separators=(',', ':')) + '\n')
    print(directory, path.stat().st_size)
