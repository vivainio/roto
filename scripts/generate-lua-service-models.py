#!/usr/bin/env python3
"""Regenerate bundled wire descriptors from installed botocore (no service logic)."""
import json
from pathlib import Path
import botocore.session
root=Path(__file__).resolve().parents[1] / 'services'
s=botocore.session.get_session()
for name in ['opensearch', 'rds']:
 model=s.get_service_data(name)
 # Keep all operation routes so unsupported operations still have their real names.
 shapes={}
 def visit(ref):
  if not ref or 'shape' not in ref:return
  n=ref['shape']
  if n in shapes:return
  shape={k:v for k,v in model['shapes'][n].items() if k!='documentation'}
  for k in ['members','member','key','value']:
   if k in shape:
    if k=='members':shape[k]={a:{b:c for b,c in v.items() if b!='documentation'} for a,v in shape[k].items()}
    else:shape[k]={a:b for a,b in shape[k].items() if a!='documentation'}
  shapes[n]=shape
  for r in shape.get('members',{}).values():visit(r)
  for k in ['member','key','value']:visit(shape.get(k))
 operations={}
 for n,o in model['operations'].items():
  operations[n]={k:v for k,v in o.items() if k in ['http','input','output']}
  visit(o.get('input'));visit(o.get('output'))
 out={'metadata':model['metadata'],'operations':operations,'shapes':shapes}
 (root/name).mkdir(exist_ok=True)
 (root/name/'model.json').write_text(json.dumps(out,separators=(',',':'))+'\n')
 print(name,(root/name/'model.json').stat().st_size)
