#!/usr/bin/env python3
"""Build an offline API coverage dashboard from Roto JSONL request traces."""

import argparse
import json
from collections import defaultdict
from pathlib import Path


def model_operations(root):
    models = {}
    for path in sorted(root.glob("*/service-2.json")):
        model = json.loads(path.read_text())
        metadata = model.get("metadata", {})
        service = metadata.get("signingName", metadata.get("endpointPrefix", path.parent.name))
        models[service] = sorted(set(models.get(service, [])) | set(model.get("operations", {})))
    return models


def all_botocore_operations():
    try:
        import botocore.session
    except ImportError as error:
        raise SystemExit("--full-spec needs botocore; use the Moto venv's Python") from error
    loader = botocore.session.get_session().get_component("data_loader")
    models = {}
    for service_name in loader.list_available_services("service-2"):
        model = loader.load_service_model(service_name, "service-2")
        metadata = model.get("metadata", {})
        signing_name = metadata.get("signingName", metadata.get("endpointPrefix", service_name))
        models[signing_name] = sorted(set(models.get(signing_name, [])) | set(model.get("operations", {})))
    return models


def read_traces(paths):
    rows = []
    for path in paths:
        with path.open() as stream:
            for number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as error:
                    raise SystemExit(f"{path}:{number}: invalid JSON: {error}")
    return rows


def build_data(models, traces):
    grouped = defaultdict(list)
    for row in traces:
        if row.get("service") and row.get("operation"):
            service = row["service"]
            grouped[(service, row["operation"])].append(row)
    services = sorted(set(models) | {service for service, _ in grouped})
    operations, summaries = [], []
    for service in services:
        names = set(models.get(service, []))
        names.update(operation for svc, operation in grouped if svc == service)
        service_ops = []
        for name in sorted(names):
            calls = grouped.get((service, name), [])
            outcomes = {call.get("outcome") for call in calls}
            status = (
                "unsupported" if "unsupported" in outcomes else
                "success" if "success" in outcomes else
                "error" if calls else "unseen"
            )
            item = {
                "service": service,
                "operation": name,
                "status": status,
                "calls": len(calls),
                "trace_ids": sorted({c["trace_id"] for c in calls if c.get("trace_id")}),
                "codes": sorted({c["error_code"] for c in calls if c.get("error_code")}),
            }
            operations.append(item)
            service_ops.append(item)
        total = len(service_ops)
        seen = sum(row["status"] != "unseen" for row in service_ops)
        summaries.append({
            "service": service,
            "total": total,
            "seen": seen,
            "success": sum(row["status"] == "success" for row in service_ops),
            "unsupported": sum(row["status"] == "unsupported" for row in service_ops),
            "calls": sum(row["calls"] for row in service_ops),
            "percent": round(100 * seen / total) if total else 0,
        })
    return {"operations": operations, "services": summaries, "trace_count": len(traces)}


PAGE = r'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Roto API coverage</title><style>
:root{font:14px/1.45 system-ui,sans-serif;color:#17212b;background:#f3f6f8}*{box-sizing:border-box}body{margin:0}header{background:#152a3b;color:white;padding:24px max(24px,calc((100vw - 1400px)/2))}h1{font-size:24px;margin:0}header p{margin:4px 0 0;color:#c6d5df}.wrap{max-width:1400px;padding:20px 24px;margin:auto}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}.card,.panel{background:white;border:1px solid #dce4e9;border-radius:9px}.card{padding:14px}.card b{display:block;font-size:24px}.card span,.hint{color:#60717e}.layout{display:grid;grid-template-columns:minmax(240px,320px) 1fr;gap:14px}.panel{padding:15px;min-width:0}h2{font-size:16px;margin:0 0 12px}.bars{max-height:70vh;overflow:auto}.bar{display:grid;grid-template-columns:100px 1fr 40px;gap:8px;align-items:center;margin:9px 0;cursor:pointer}.bar label{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer}.track{height:9px;background:#e9eef1;border-radius:9px;overflow:hidden}.fill{height:100%;background:#3585a2}.bar small{text-align:right;color:#60717e}.controls{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}input,select{font:inherit;padding:8px;border:1px solid #cbd6dd;border-radius:6px;background:white}input{flex:1;min-width:170px}table{width:100%;border-collapse:collapse}th{text-align:left;color:#647581;font-size:12px;position:sticky;top:0;background:white}td,th{padding:8px;border-bottom:1px solid #edf0f2}td:first-child{font-family:ui-monospace,monospace}.tablewrap{max-height:64vh;overflow:auto}.badge{display:inline-block;border-radius:99px;padding:2px 8px;font-size:12px}.success{background:#e3f5e9;color:#176539}.unsupported{background:#ffe8e5;color:#9c2e22}.error{background:#fff1d6;color:#815600}.unseen{background:#edf0f2;color:#596873}tr.click{cursor:pointer}tr.click:hover{background:#f6f9fa}#detail{display:none;margin-top:12px;padding:12px;background:#f5f8fa;border-radius:7px;white-space:pre-wrap;overflow-wrap:anywhere}.hint{font-size:12px}@media(max-width:800px){.layout{grid-template-columns:1fr}.cards{grid-template-columns:repeat(2,1fr)}.bars{max-height:280px}}
</style><header><h1>Roto API coverage</h1><p id="subtitle"></p></header><main class="wrap"><section class="cards" id="cards"></section><div class="layout"><section class="panel"><h2>Coverage by service</h2><div class="bars" id="bars"></div><p class="hint">Observed means the operation appeared in a trace; it does not imply complete behavior.</p></section><section class="panel"><h2>Operations</h2><div class="controls"><input id="search" placeholder="Filter service, operation, or trace ID"><select id="status"><option value="all">All statuses</option><option value="unseen">Unseen</option><option value="success">Observed success</option><option value="error">Observed errors</option><option value="unsupported">Unsupported</option></select><select id="service"><option value="all">All services</option></select></div><div class="tablewrap"><table><thead><tr><th>Operation</th><th>Status</th><th>Calls</th><th>IDs</th></tr></thead><tbody id="rows"></tbody></table></div><div id="detail"></div></section></div></main>
<script>const D=__DATA__,esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));const O=D.operations;document.querySelector('#subtitle').textContent=`${D.trace_count.toLocaleString()} traced requests · ${O.length.toLocaleString()} modeled operations · ${D.services.length} services`;const seen=O.filter(x=>x.status!=='unseen').length,uns=O.filter(x=>x.status==='unsupported').length,ids=new Set(O.flatMap(x=>x.trace_ids)).size;document.querySelector('#cards').innerHTML=[[`${seen}/${O.length}`,'operations observed'],[`${Math.round(100*seen/Math.max(O.length,1))}%`,'spec operations seen'],[uns,'unsupported operations'],[ids,'correlation IDs']].map(([n,l])=>`<div class="card"><b>${esc(n)}</b><span>${esc(l)}</span></div>`).join('');const S=document.querySelector('#service');for(const s of D.services)S.insertAdjacentHTML('beforeend',`<option value="${esc(s.service)}">${esc(s.service)}</option>`);const B=document.querySelector('#bars');B.innerHTML=D.services.map(s=>`<div class="bar" data-s="${esc(s.service)}"><label>${esc(s.service)}</label><div class="track"><div class="fill" style="width:${s.percent}%"></div></div><small>${s.percent}%</small></div>`).join('');B.onclick=e=>{const b=e.target.closest('.bar');if(b){S.value=b.dataset.s;render()}};function render(){const q=document.querySelector('#search').value.toLowerCase(),st=document.querySelector('#status').value,sv=S.value,rows=O.filter(x=>(st==='all'||x.status===st)&&(sv==='all'||x.service===sv)&&(!q||`${x.service} ${x.operation} ${x.trace_ids.join(' ')}`.toLowerCase().includes(q)));document.querySelector('#rows').innerHTML=rows.map(x=>`<tr class="click" data-key="${O.indexOf(x)}"><td>${esc(x.service)} · ${esc(x.operation)}</td><td><span class="badge ${x.status}">${x.status==='success'?'observed success':x.status==='error'?'observed errors':x.status}</span></td><td>${x.calls}</td><td>${x.trace_ids.length}</td></tr>`).join('');document.querySelectorAll('tr.click').forEach(r=>r.onclick=()=>{const x=O[+r.dataset.key],d=document.querySelector('#detail');d.style.display='block';d.textContent=`${x.service}.${x.operation}\nStatus: ${x.status} · Calls: ${x.calls}\n${x.codes.length?'Error codes: '+x.codes.join(', ')+'\n':''}${x.trace_ids.length?'Trace IDs:\n'+x.trace_ids.join('\n'):'No trace ID was present in these requests.'}`})}document.querySelectorAll('.controls input,.controls select').forEach(x=>x.addEventListener('input',render));render();</script></html>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("traces", nargs="+", type=Path, help="Roto --trace JSONL file(s)")
    parser.add_argument("--models", type=Path, default=Path(__file__).resolve().parents[1] / "models")
    parser.add_argument("--full-spec", action="store_true", help="load every AWS service model from installed botocore")
    parser.add_argument("--output", type=Path, default=Path("api-coverage.html"))
    args = parser.parse_args()
    models = all_botocore_operations() if args.full_spec else model_operations(args.models)
    data = build_data(models, read_traces(args.traces))
    encoded = json.dumps(data, separators=(",", ":")).replace("<", "\\u003c")
    args.output.write_text(PAGE.replace("__DATA__", encoded))
    print(f"Wrote {args.output}: {len(data['operations'])} operations, {data['trace_count']} calls")


if __name__ == "__main__":
    main()
