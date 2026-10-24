# Elastic Load Balancing v2

Roto simulates application load balancers in two parts. The control plane is a Luau service
(`services/elbv2`, signing name `elasticloadbalancing`) that holds load balancers, target groups,
listeners, rules, registered targets and tags, so Terraform, CDK and the SDKs work against it. The
data plane is optional: start the server with `--alb-port` and it forwards real HTTP requests to the
registered targets and runs their health checks.

```sh
roto-server --lua-services services --alb-port 8888
```

The Luau service bundle (`--lua-services`) is required for both parts. The control plane keeps all
state; the proxy only reads it, and writes target health back so `DescribeTargetHealth` reports it.

## Control plane

Supported operations: `Create`/`Describe`/`DeleteLoadBalancer`, load balancer attributes,
`SetSecurityGroups`, `SetSubnets`, `SetIpAddressType`; `Create`/`Describe`/`Modify`/`DeleteTargetGroup`,
target group attributes; `RegisterTargets`, `DeregisterTargets`, `DescribeTargetHealth`;
`Create`/`Describe`/`Modify`/`DeleteListener`, `DescribeListenerCertificates`;
`Create`/`Describe`/`Modify`/`DeleteRule`, `SetRulePriorities`; `AddTags`, `RemoveTags`,
`DescribeTags`; `DescribeSSLPolicies` and `DescribeAccountLimits` (static). Other operations return
`NotImplemented`.

Conditions are `path-pattern`, `host-header`, `http-header`, `http-request-method`, `query-string`
and `source-ip`. Actions are `forward` (single group or weighted `ForwardConfig`),
`fixed-response` and `redirect`. Authentication actions are stored but not performed.

Load balancers get a DNS name such as `web-347206579.us-east-1.elb.localhost`. `*.localhost` names
resolve to the loopback address in most clients, so `curl http://web-347206579.us-east-1.elb.localhost:8888/`
reaches the proxy without a `Host` header override.

Target health is `initial` until the group is used by a listener or rule of a load balancer
(`unused` otherwise), then follows the group's health check settings.

## Data plane

One port serves every load balancer. A request is routed as follows.

1. **Load balancer.** The `Host` header names it by DNS name or by the leading label of that name.
   `x-roto-load-balancer: <name>` selects one explicitly. A request for any other host reaches the
   only application load balancer, when there is exactly one.
2. **Listener.** The port in the `Host` header (`name.elb.localhost:80`) selects the listener with
   that port. Without it, the lowest-numbered HTTP listener is used.
3. **Rule.** Rules are tried by priority, all conditions of a rule must match, and the default
   action runs when none does.
4. **Target.** Round-robin over healthy targets. Like an ALB, when no target is healthy the request
   goes to all registered targets.

The proxy adds `X-Forwarded-For` (appended), `X-Forwarded-Proto`, `X-Forwarded-Port` and
`X-Amzn-Trace-Id`, and preserves the `Host` header. It answers `502` when a target refuses the
connection, `504` after 60 seconds, and `503` when the group has no targets or no positive weight.

Health checks request `HealthCheckPath` from each target of every group in use, every
`HealthCheckIntervalSeconds`. A target becomes `healthy` after `HealthyThresholdCount` consecutive
passes and `unhealthy` after `UnhealthyThresholdCount` failures, with the usual `Target.*` reasons.
The checker polls once a second, so use short intervals (the minimum is 5 seconds) in tests.

### Limits

- Targets must be reachable from the roto process. The target `Id` is used as the host, so use
  target type `ip` (or `instance` with a resolvable name); `lambda` and `alb` targets answer `503`.
- TLS is not terminated. An HTTPS listener (which needs a certificate ARN, not validated against
  ACM) is served as plain HTTP on the proxy port, with `X-Forwarded-Proto: https`.
- Request and response bodies are buffered (64 MiB request limit); there is no WebSocket upgrade.
- No sticky sessions, slow start, deregistration delay or cross-zone behaviour. Network and gateway
  load balancers can be created but are not served.

Run `.venv-moto/bin/python tests/smoke-elbv2.py` for an SDK walkthrough of both parts.
