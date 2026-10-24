//! Application load balancer data plane: a forwarding proxy and target health checker for the
//! `elasticloadbalancing` Luau service (`services/elbv2/service.luau`).
//!
//! The Luau service owns all configuration. This module only reads its documents from the service
//! database (namespaces `<account>/<region>/elbv2/{lb,tg,listener,rule,health}`) and writes target
//! health back, so there is no second copy of any state to drift.
//!
//! Scope: HTTP/1.1 forwarding to `ip` and `instance` targets (the target Id is used as the host).
//! Listeners of any protocol are served as plain HTTP on the one proxy port; TLS is not terminated.
//! Lambda and ALB targets, authentication actions and sticky sessions are not implemented.
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use roto_core::AwsError;
use roto_core::rusqlite::params;
use roto_core::store::{Db, Store};
use serde_json::{Value, json};

/// SigV4 signing name of the Luau service, which is also the key of its database.
pub const SERVICE: &str = "elasticloadbalancing";
const MAX_BODY: usize = 64 * 1024 * 1024;
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------------------------
// State read from the Luau service
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Balancer {
    arn: String,
    name: String,
    dns: String,
    kind: String,
}

#[derive(Debug, Clone)]
struct Listener {
    arn: String,
    balancer: String,
    port: u16,
    protocol: String,
    actions: Vec<Action>,
}

#[derive(Debug, Clone)]
struct Rule {
    listener: String,
    priority: u32,
    conditions: Vec<Condition>,
    actions: Vec<Action>,
}

#[derive(Debug, Clone, PartialEq)]
enum Action {
    Forward(Vec<(String, u32)>),
    Fixed {
        status: u16,
        content_type: String,
        body: String,
    },
    Redirect(Redirect),
    /// Authentication and any other action this proxy cannot perform.
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq)]
struct Redirect {
    protocol: String,
    host: String,
    port: String,
    path: String,
    query: String,
    status: u16,
}

#[derive(Debug, Clone, PartialEq)]
enum Condition {
    Path(Vec<String>),
    Host(Vec<String>),
    Header(String, Vec<String>),
    Method(Vec<String>),
    Query(Vec<(Option<String>, String)>),
    SourceIp(Vec<String>),
}

#[derive(Debug, Clone)]
struct Target {
    id: String,
    port: u16,
}

#[derive(Debug, Clone)]
struct Group {
    arn: String,
    kind: String,
    targets: Vec<Target>,
    check: HealthCheck,
    /// Namespace prefix shared by this group's account and region, e.g. `123/us-east-1/elbv2/`.
    prefix: String,
}

#[derive(Debug, Clone)]
struct HealthCheck {
    enabled: bool,
    protocol: String,
    /// `None` is "traffic-port".
    port: Option<u16>,
    path: String,
    interval: Duration,
    timeout: Duration,
    healthy: u32,
    unhealthy: u32,
    matcher: String,
}

#[derive(Debug, Clone)]
struct Health {
    state: String,
    reason: Option<String>,
}

#[derive(Default)]
struct Snapshot {
    balancers: Vec<Balancer>,
    listeners: Vec<Listener>,
    rules: Vec<Rule>,
    groups: HashMap<String, Group>,
    /// Keyed by `<group arn>|<id>:<port>`, as the Luau service writes them.
    health: HashMap<String, Health>,
    /// Namespace of each health document, for pruning.
    health_namespaces: HashMap<String, String>,
}

fn text<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

fn number(v: &Value, key: &str) -> Option<u64> {
    v.get(key).and_then(Value::as_u64)
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_actions(v: &Value) -> Vec<Action> {
    let mut actions: Vec<(u64, Action)> = Vec::new();
    for (index, a) in v.as_array().into_iter().flatten().enumerate() {
        let order = number(a, "Order").unwrap_or(index as u64 + 1);
        let action = match text(a, "Type") {
            Some("forward") => {
                let mut groups = Vec::new();
                for g in a["ForwardConfig"]["TargetGroups"]
                    .as_array()
                    .into_iter()
                    .flatten()
                {
                    if let Some(arn) = text(g, "TargetGroupArn") {
                        groups.push((arn.to_owned(), number(g, "Weight").unwrap_or(1) as u32));
                    }
                }
                Action::Forward(groups)
            }
            Some("fixed-response") => {
                let c = &a["FixedResponseConfig"];
                Action::Fixed {
                    status: text(c, "StatusCode")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(503),
                    content_type: text(c, "ContentType").unwrap_or("text/plain").to_owned(),
                    body: text(c, "MessageBody").unwrap_or("").to_owned(),
                }
            }
            Some("redirect") => {
                let c = &a["RedirectConfig"];
                let field = |k: &str, d: &str| text(c, k).unwrap_or(d).to_owned();
                Action::Redirect(Redirect {
                    protocol: field("Protocol", "#{protocol}"),
                    host: field("Host", "#{host}"),
                    port: field("Port", "#{port}"),
                    path: field("Path", "/#{path}"),
                    query: field("Query", "#{query}"),
                    status: if text(c, "StatusCode") == Some("HTTP_302") {
                        302
                    } else {
                        301
                    },
                })
            }
            other => Action::Unsupported(other.unwrap_or("missing").to_owned()),
        };
        actions.push((order, action));
    }
    actions.sort_by_key(|(order, _)| *order);
    actions.into_iter().map(|(_, a)| a).collect()
}

fn parse_conditions(v: &Value) -> Vec<Condition> {
    let mut out = Vec::new();
    for c in v.as_array().into_iter().flatten() {
        let condition = match text(c, "Field") {
            Some("path-pattern") => Condition::Path(strings(&c["PathPatternConfig"]["Values"])),
            Some("host-header") => Condition::Host(strings(&c["HostHeaderConfig"]["Values"])),
            Some("http-header") => Condition::Header(
                text(&c["HttpHeaderConfig"], "HttpHeaderName")
                    .unwrap_or("")
                    .to_owned(),
                strings(&c["HttpHeaderConfig"]["Values"]),
            ),
            Some("http-request-method") => {
                Condition::Method(strings(&c["HttpRequestMethodConfig"]["Values"]))
            }
            Some("query-string") => Condition::Query(
                c["QueryStringConfig"]["Values"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|p| {
                        (
                            text(p, "Key").map(str::to_owned),
                            text(p, "Value").unwrap_or("").to_owned(),
                        )
                    })
                    .collect(),
            ),
            Some("source-ip") => Condition::SourceIp(strings(&c["SourceIpConfig"]["Values"])),
            // An unknown field can never match, which is safer than ignoring it.
            _ => Condition::Method(Vec::new()),
        };
        out.push(condition);
    }
    out
}

fn parse_matcher_and_check(v: &Value) -> HealthCheck {
    HealthCheck {
        enabled: v
            .get("HealthCheckEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        protocol: text(v, "HealthCheckProtocol").unwrap_or("HTTP").to_owned(),
        port: text(v, "HealthCheckPort").and_then(|p| p.parse().ok()),
        path: text(v, "HealthCheckPath").unwrap_or("/").to_owned(),
        interval: Duration::from_secs(number(v, "HealthCheckIntervalSeconds").unwrap_or(30)),
        timeout: Duration::from_secs(number(v, "HealthCheckTimeoutSeconds").unwrap_or(5)),
        healthy: number(v, "HealthyThresholdCount").unwrap_or(5) as u32,
        unhealthy: number(v, "UnhealthyThresholdCount").unwrap_or(2) as u32,
        matcher: text(&v["Matcher"], "HttpCode").unwrap_or("200").to_owned(),
    }
}

impl Snapshot {
    fn load(db: &Db) -> Result<Self, AwsError> {
        let rows: Vec<(String, String)> = db.read(|conn| {
            let mut statement = conn.prepare(
                "SELECT namespace, body FROM documents WHERE namespace LIKE '%/elbv2/%'",
            )?;
            let rows = statement
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        let mut snapshot = Snapshot::default();
        for (namespace, body) in rows {
            let Ok(v) = serde_json::from_str::<Value>(&body) else {
                continue;
            };
            let Some((prefix, kind)) = namespace.rsplit_once('/') else {
                continue;
            };
            let prefix = format!("{prefix}/");
            match kind {
                "lb" => snapshot.balancers.push(Balancer {
                    arn: text(&v, "LoadBalancerArn").unwrap_or("").to_owned(),
                    name: text(&v, "LoadBalancerName").unwrap_or("").to_owned(),
                    dns: text(&v, "DNSName").unwrap_or("").to_ascii_lowercase(),
                    kind: text(&v, "Type").unwrap_or("application").to_owned(),
                }),
                "listener" => snapshot.listeners.push(Listener {
                    arn: text(&v, "ListenerArn").unwrap_or("").to_owned(),
                    balancer: text(&v, "LoadBalancerArn").unwrap_or("").to_owned(),
                    port: number(&v, "Port").unwrap_or(0) as u16,
                    protocol: text(&v, "Protocol").unwrap_or("HTTP").to_owned(),
                    actions: parse_actions(&v["DefaultActions"]),
                }),
                "rule" => snapshot.rules.push(Rule {
                    listener: text(&v, "_ListenerArn").unwrap_or("").to_owned(),
                    priority: text(&v, "Priority")
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(u32::MAX),
                    conditions: parse_conditions(&v["Conditions"]),
                    actions: parse_actions(&v["Actions"]),
                }),
                "tg" => {
                    let arn = text(&v, "TargetGroupArn").unwrap_or("").to_owned();
                    let targets = v["_Targets"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|t| {
                            Some(Target {
                                id: text(t, "Id")?.to_owned(),
                                port: number(t, "Port").unwrap_or(0) as u16,
                            })
                        })
                        .collect();
                    snapshot.groups.insert(
                        arn.clone(),
                        Group {
                            arn,
                            kind: text(&v, "TargetType").unwrap_or("instance").to_owned(),
                            targets,
                            check: parse_matcher_and_check(&v),
                            prefix,
                        },
                    );
                }
                _ => {}
            }
        }
        // Health documents are keyed by id, which the body does not repeat.
        let health_rows: Vec<(String, String, String)> = db.read(|conn| {
            let mut statement = conn.prepare(
                "SELECT namespace, id, body FROM documents WHERE namespace LIKE '%/elbv2/health'",
            )?;
            let rows = statement
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })?;
        for (namespace, id, body) in health_rows {
            let Ok(v) = serde_json::from_str::<Value>(&body) else {
                continue;
            };
            snapshot.health.insert(
                id.clone(),
                Health {
                    state: text(&v, "State").unwrap_or("initial").to_owned(),
                    reason: text(&v, "Reason").map(str::to_owned),
                },
            );
            snapshot.health_namespaces.insert(id, namespace);
        }
        Ok(snapshot)
    }

    /// Groups that some listener or rule of an application load balancer sends traffic to.
    fn groups_in_use(&self) -> HashSet<&str> {
        let applications: HashSet<&str> = self
            .balancers
            .iter()
            .filter(|b| b.kind == "application")
            .map(|b| b.arn.as_str())
            .collect();
        let live: HashSet<&str> = self
            .listeners
            .iter()
            .filter(|l| applications.contains(l.balancer.as_str()))
            .map(|l| l.arn.as_str())
            .collect();
        fn note<'a>(used: &mut HashSet<&'a str>, actions: &'a [Action]) {
            for action in actions {
                if let Action::Forward(groups) = action {
                    used.extend(groups.iter().map(|(arn, _)| arn.as_str()));
                }
            }
        }
        let mut used = HashSet::new();
        for l in self
            .listeners
            .iter()
            .filter(|l| live.contains(l.arn.as_str()))
        {
            note(&mut used, &l.actions);
        }
        for r in self
            .rules
            .iter()
            .filter(|r| live.contains(r.listener.as_str()))
        {
            note(&mut used, &r.actions);
        }
        used
    }
}

fn target_key(group: &str, target: &Target) -> String {
    format!("{group}|{}:{}", target.id, target.port)
}

// ---------------------------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------------------------

/// `*` matches any run of characters and `?` exactly one, as in ALB condition values.
fn wildcard(pattern: &str, value: &str, ignore_case: bool) -> bool {
    let (p, v): (Vec<char>, Vec<char>) = if ignore_case {
        (
            pattern.to_lowercase().chars().collect(),
            value.to_lowercase().chars().collect(),
        )
    } else {
        (pattern.chars().collect(), value.chars().collect())
    };
    let (mut pi, mut vi) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == v[vi]) && p[pi] != '*' {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = vi;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            vi = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn cidr_contains(cidr: &str, ip: IpAddr) -> bool {
    let (network, bits) = match cidr.split_once('/') {
        Some((n, b)) => (n, b.parse::<u32>().ok()),
        None => (cidr, None),
    };
    let Ok(network) = network.parse::<IpAddr>() else {
        return false;
    };
    match (network, ip) {
        (IpAddr::V4(n), IpAddr::V4(i)) => {
            let bits = bits.unwrap_or(32).min(32);
            let mask = if bits == 0 {
                0
            } else {
                u32::MAX << (32 - bits)
            };
            u32::from(n) & mask == u32::from(i) & mask
        }
        (IpAddr::V6(n), IpAddr::V6(i)) => {
            let bits = bits.unwrap_or(128).min(128);
            let mask = if bits == 0 {
                0
            } else {
                u128::MAX << (128 - bits)
            };
            u128::from(n) & mask == u128::from(i) & mask
        }
        _ => false,
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let decoded = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok());
                match decoded {
                    Some(b) => {
                        out.push(b);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The parts of a request that conditions look at.
struct RequestView<'a> {
    method: &'a str,
    host: &'a str,
    path: &'a str,
    query: &'a str,
    headers: &'a HeaderMap,
    source: IpAddr,
}

impl Condition {
    fn matches(&self, r: &RequestView) -> bool {
        match self {
            Condition::Path(values) => values.iter().any(|v| wildcard(v, r.path, false)),
            Condition::Host(values) => values.iter().any(|v| wildcard(v, r.host, true)),
            Condition::Header(name, values) => r
                .headers
                .get_all(name.as_str())
                .iter()
                .filter_map(|h| h.to_str().ok())
                .any(|h| values.iter().any(|v| wildcard(v, h, true))),
            Condition::Method(values) => values.iter().any(|v| v == r.method),
            Condition::Query(pairs) => {
                let request: Vec<(String, String)> = r
                    .query
                    .split('&')
                    .filter(|p| !p.is_empty())
                    .map(|p| {
                        let (k, v) = p.split_once('=').unwrap_or((p, ""));
                        (percent_decode(k), percent_decode(v))
                    })
                    .collect();
                pairs.iter().any(|(key, value)| {
                    request.iter().any(|(k, v)| {
                        key.as_ref().is_none_or(|key| wildcard(key, k, true))
                            && wildcard(value, v, true)
                    })
                })
            }
            Condition::SourceIp(values) => values.iter().any(|c| cidr_contains(c, r.source)),
        }
    }
}

/// Rules in priority order, then the listener's default actions.
fn pick_actions<'a>(
    snapshot: &'a Snapshot,
    listener: &'a Listener,
    request: &RequestView,
) -> &'a [Action] {
    let mut rules: Vec<&Rule> = snapshot
        .rules
        .iter()
        .filter(|r| r.listener == listener.arn)
        .collect();
    rules.sort_by_key(|r| r.priority);
    rules
        .into_iter()
        .find(|r| r.conditions.iter().all(|c| c.matches(request)))
        .map(|r| r.actions.as_slice())
        .unwrap_or(&listener.actions)
}

fn split_host(value: &str) -> (String, Option<u16>) {
    let value = value.trim().to_ascii_lowercase();
    if let Some((host, tail)) = value
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']'))
    {
        return (
            format!("[{host}]"),
            tail.strip_prefix(':').and_then(|p| p.parse().ok()),
        );
    }
    match value.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) && !port.is_empty() => {
            (host.to_owned(), port.parse().ok())
        }
        _ => (value, None),
    }
}

fn find_balancer<'a>(
    snapshot: &'a Snapshot,
    host: &str,
    header: Option<&str>,
) -> Option<&'a Balancer> {
    let applications: Vec<&Balancer> = snapshot
        .balancers
        .iter()
        .filter(|b| b.kind == "application")
        .collect();
    if let Some(name) = header {
        return applications.into_iter().find(|b| b.name == name);
    }
    let label = host.split('.').next().unwrap_or("");
    applications
        .iter()
        .find(|b| b.dns == host)
        .or_else(|| {
            applications
                .iter()
                .find(|b| b.dns.split('.').next() == Some(label))
        })
        .or(if applications.len() == 1 && !host.contains(".elb.") {
            applications.first()
        } else {
            None
        })
        .copied()
}

fn find_listener<'a>(
    snapshot: &'a Snapshot,
    balancer: &Balancer,
    port: Option<u16>,
) -> Option<&'a Listener> {
    let mut listeners: Vec<&Listener> = snapshot
        .listeners
        .iter()
        .filter(|l| l.balancer == balancer.arn)
        .collect();
    listeners.sort_by_key(|l| l.port);
    port.and_then(|p| listeners.iter().find(|l| l.port == p).copied())
        .or_else(|| listeners.iter().find(|l| l.protocol == "HTTP").copied())
        .or_else(|| listeners.first().copied())
}

// ---------------------------------------------------------------------------------------------
// Proxy
// ---------------------------------------------------------------------------------------------

struct Proxy {
    store: Arc<Store>,
    client: reqwest::Client,
    round_robin: AtomicU64,
}

static RANDOM: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

fn random() -> u64 {
    let seed = RANDOM.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
    let mut z = seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn plain(status: u16, message: &str) -> Response {
    let mut response = (
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        message.to_owned(),
    )
        .into_response();
    response
        .headers_mut()
        .insert("server", HeaderValue::from_static("awselb/2.0"));
    response
}

const HOP_BY_HOP: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn expand(
    template: &str,
    protocol: &str,
    host: &str,
    port: u16,
    path: &str,
    query: &str,
) -> String {
    template
        .replace("#{protocol}", protocol)
        .replace("#{host}", host)
        .replace("#{port}", &port.to_string())
        .replace("#{path}", path.trim_start_matches('/'))
        .replace("#{query}", query)
}

fn redirect_location(
    r: &Redirect,
    protocol: &str,
    host: &str,
    port: u16,
    path: &str,
    query: &str,
) -> String {
    let protocol = expand(&r.protocol, protocol, host, port, path, query).to_lowercase();
    let host = expand(&r.host, &protocol, host, port, path, query);
    let port = expand(&r.port, &protocol, &host, port, path, query);
    let path = expand(&r.path, &protocol, &host, 0, path, query);
    let query = expand(&r.query, &protocol, &host, 0, "", query);
    let mut location = format!("{protocol}://{host}:{port}{path}");
    if !query.is_empty() {
        location.push('?');
        location.push_str(&query);
    }
    location
}

async fn handle(
    State(proxy): State<Arc<Proxy>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response {
    let Some(db) = proxy.store.existing_db(SERVICE) else {
        return plain(503, "no load balancers are configured");
    };
    let snapshot = match tokio::task::spawn_blocking(move || Snapshot::load(&db)).await {
        Ok(Ok(s)) => s,
        _ => return plain(500, "cannot read load balancer state"),
    };
    let (parts, body) = request.into_parts();
    let host_header = parts
        .headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .or_else(|| parts.uri.authority().map(|a| a.as_str()))
        .unwrap_or("");
    let (host, host_port) = split_host(host_header);
    let selector = parts
        .headers
        .get("x-roto-load-balancer")
        .and_then(|h| h.to_str().ok());
    let Some(balancer) = find_balancer(&snapshot, &host, selector) else {
        return plain(404, "no load balancer matches this host");
    };
    let Some(listener) = find_listener(&snapshot, balancer, host_port) else {
        return plain(503, "the load balancer has no listeners");
    };
    let path = parts.uri.path().to_owned();
    let query = parts.uri.query().unwrap_or("").to_owned();
    let view = RequestView {
        method: parts.method.as_str(),
        host: &host,
        path: &path,
        query: &query,
        headers: &parts.headers,
        source: peer.ip(),
    };
    let actions = pick_actions(&snapshot, listener, &view);
    let proto = listener.protocol.to_ascii_lowercase();

    let weighted = match actions.first() {
        Some(Action::Forward(weighted)) => weighted,
        Some(Action::Fixed {
            status,
            content_type,
            body,
        }) => {
            let mut response = plain(*status, body);
            if let Ok(value) = HeaderValue::from_str(content_type) {
                response.headers_mut().insert("content-type", value);
            }
            return response;
        }
        Some(Action::Redirect(r)) => {
            let location = redirect_location(r, &proto, &host, listener.port, &path, &query);
            let mut response = plain(r.status, "");
            if let Ok(value) = HeaderValue::from_str(&location) {
                response.headers_mut().insert("location", value);
            }
            return response;
        }
        Some(Action::Unsupported(kind)) => {
            return plain(500, &format!("unsupported listener action: {kind}"));
        }
        None => return plain(500, "listener has no action"),
    };
    let total: u64 = weighted.iter().map(|(_, w)| u64::from(*w)).sum();
    if total == 0 {
        return plain(503, "no target group has a positive weight");
    }
    let mut roll = random() % total;
    let mut chosen = &weighted[0].0;
    for (arn, weight) in weighted {
        if roll < u64::from(*weight) {
            chosen = arn;
            break;
        }
        roll -= u64::from(*weight);
    }
    let Some(group) = snapshot.groups.get(chosen) else {
        return plain(503, "target group not found");
    };
    if group.kind != "ip" && group.kind != "instance" {
        return plain(
            503,
            &format!(
                "target type {} is not supported by this load balancer",
                group.kind
            ),
        );
    }
    let healthy: Vec<&Target> = group
        .targets
        .iter()
        .filter(|t| {
            snapshot
                .health
                .get(&target_key(&group.arn, t))
                .is_some_and(|h| h.state == "healthy")
        })
        .collect();
    // Like an ALB, route to every registered target when none is healthy.
    let candidates: Vec<&Target> = if healthy.is_empty() {
        group.targets.iter().collect()
    } else {
        healthy
    };
    if candidates.is_empty() {
        return plain(503, "the target group has no registered targets");
    }
    let target =
        candidates[(proxy.round_robin.fetch_add(1, Ordering::Relaxed) as usize) % candidates.len()];

    let body = match axum::body::to_bytes(body, MAX_BODY).await {
        Ok(b) => b,
        Err(_) => return plain(413, "request body too large"),
    };
    let authority = if target.id.contains(':') && !target.id.starts_with('[') {
        format!("[{}]:{}", target.id, target.port)
    } else {
        format!("{}:{}", target.id, target.port)
    };
    let url = format!(
        "http://{authority}{}",
        parts
            .uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
    );
    let mut forwarded = reqwest::header::HeaderMap::new();
    for (name, value) in &parts.headers {
        let lower = name.as_str();
        if HOP_BY_HOP.contains(&lower) || lower == "content-length" || lower == "x-forwarded-for" {
            continue;
        }
        forwarded.append(name.clone(), value.clone());
    }
    let client_ip = match parts
        .headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
    {
        Some(existing) => format!("{existing}, {}", peer.ip()),
        None => peer.ip().to_string(),
    };
    forwarded.insert(
        "x-forwarded-for",
        HeaderValue::from_str(&client_ip).unwrap(),
    );
    forwarded.insert("x-forwarded-proto", HeaderValue::from_str(&proto).unwrap());
    forwarded.insert(
        "x-forwarded-port",
        HeaderValue::from_str(&listener.port.to_string()).unwrap(),
    );
    let trace = format!(
        "Root=1-{:08x}-{:024x}",
        random() as u32,
        u128::from(random()) << 32 | u128::from(random() as u32)
    );
    forwarded.insert("x-amzn-trace-id", HeaderValue::from_str(&trace).unwrap());

    let upstream = proxy
        .client
        .request(parts.method.clone(), &url)
        .headers(forwarded)
        .body(body)
        .send()
        .await;
    match upstream {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            let bytes = match response.bytes().await {
                Ok(b) => b,
                Err(_) => return plain(502, "target closed the connection"),
            };
            let mut out = Response::new(Body::from(bytes));
            *out.status_mut() = status;
            for (name, value) in &headers {
                let lower = name.as_str();
                if HOP_BY_HOP.contains(&lower) || lower == "content-length" {
                    continue;
                }
                if let (Ok(n), Ok(v)) = (
                    HeaderName::from_bytes(name.as_str().as_bytes()),
                    HeaderValue::from_bytes(value.as_bytes()),
                ) {
                    out.headers_mut().append(n, v);
                }
            }
            out
        }
        Err(e) if e.is_timeout() => plain(504, "target did not respond in time"),
        Err(_) => plain(502, "cannot connect to the target"),
    }
}

// ---------------------------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------------------------

fn matcher_accepts(matcher: &str, status: u16) -> bool {
    matcher.split(',').any(|part| {
        let part = part.trim();
        match part.split_once('-') {
            Some((lo, hi)) => matches!((lo.parse::<u16>(), hi.parse::<u16>()), (Ok(lo), Ok(hi)) if (lo..=hi).contains(&status)),
            None => part.parse::<u16>().is_ok_and(|code| code == status),
        }
    })
}

struct Track {
    state: String,
    reason: Option<String>,
    description: Option<String>,
    passes: u32,
    failures: u32,
    last_check: Option<Instant>,
    /// What the database holds for this target, so unchanged results are not rewritten.
    written: Option<(String, Option<String>)>,
}

enum Outcome {
    Pass,
    Fail(&'static str, String),
}

async fn check_target(client: &reqwest::Client, group: &Group, target: &Target) -> Outcome {
    let port = group.check.port.unwrap_or(target.port);
    let scheme = if group.check.protocol == "HTTPS" {
        "https"
    } else {
        "http"
    };
    let host = if target.id.contains(':') {
        format!("[{}]", target.id)
    } else {
        target.id.clone()
    };
    let url = format!("{scheme}://{host}:{port}{}", group.check.path);
    match client.get(&url).timeout(group.check.timeout).send().await {
        Ok(response) => {
            let code = response.status().as_u16();
            if matcher_accepts(&group.check.matcher, code) {
                Outcome::Pass
            } else {
                Outcome::Fail(
                    "Target.ResponseCodeMismatch",
                    format!("Health checks failed with these codes: [{code}]"),
                )
            }
        }
        Err(e) if e.is_timeout() => Outcome::Fail("Target.Timeout", "Request timed out".into()),
        Err(_) => Outcome::Fail("Target.FailedHealthChecks", "Health checks failed".into()),
    }
}

async fn health_tick(
    store: &Arc<Store>,
    client: &reqwest::Client,
    tracks: &mut HashMap<String, Track>,
) {
    let Some(db) = store.existing_db(SERVICE) else {
        return;
    };
    let loader = db.clone();
    let Ok(Ok(snapshot)) = tokio::task::spawn_blocking(move || Snapshot::load(&loader)).await
    else {
        return;
    };
    let used = snapshot.groups_in_use();
    let mut live: HashSet<String> = HashSet::new();
    let mut due: Vec<(String, Group, Target)> = Vec::new();
    for group in snapshot.groups.values() {
        for target in &group.targets {
            let key = target_key(&group.arn, target);
            live.insert(key.clone());
            if !used.contains(group.arn.as_str())
                || !group.check.enabled
                || !matches!(group.check.protocol.as_str(), "HTTP" | "HTTPS")
                || !matches!(group.kind.as_str(), "ip" | "instance")
            {
                continue;
            }
            let stored = snapshot.health.get(&key);
            let track = tracks.entry(key.clone()).or_insert_with(|| Track {
                state: stored.map_or("initial".into(), |h| h.state.clone()),
                reason: stored.and_then(|h| h.reason.clone()),
                description: None,
                passes: 0,
                failures: 0,
                last_check: None,
                written: stored.map(|h| (h.state.clone(), h.reason.clone())),
            });
            // Re-registration clears the document; start that target's checks over.
            if stored.is_none() && track.written.is_some() {
                *track = Track {
                    state: "initial".into(),
                    reason: None,
                    description: None,
                    passes: 0,
                    failures: 0,
                    last_check: None,
                    written: None,
                };
            }
            if track
                .last_check
                .is_none_or(|t| t.elapsed() >= group.check.interval)
            {
                track.last_check = Some(Instant::now());
                due.push((key, group.clone(), target.clone()));
            }
        }
    }
    tracks.retain(|key, _| live.contains(key));

    let mut checks = tokio::task::JoinSet::new();
    for (key, group, target) in due {
        let client = client.clone();
        checks.spawn(async move {
            let outcome = check_target(&client, &group, &target).await;
            (key, group, outcome)
        });
    }
    let mut writes: Vec<(String, String, Value)> = Vec::new();
    while let Some(Ok((key, group, outcome))) = checks.join_next().await {
        let Some(track) = tracks.get_mut(&key) else {
            continue;
        };
        match outcome {
            Outcome::Pass => {
                track.passes += 1;
                track.failures = 0;
                if track.passes >= group.check.healthy {
                    track.state = "healthy".into();
                    track.reason = None;
                    track.description = None;
                } else if track.state == "initial" {
                    track.reason = Some("Elb.InitialHealthChecking".into());
                    track.description = Some("Initial health checks in progress".into());
                }
            }
            Outcome::Fail(reason, detail) => {
                track.failures += 1;
                track.passes = 0;
                if track.failures >= group.check.unhealthy {
                    track.state = "unhealthy".into();
                    track.reason = Some(reason.into());
                    track.description = Some(detail);
                } else if track.state == "initial" {
                    track.reason = Some("Elb.InitialHealthChecking".into());
                    track.description = Some("Initial health checks in progress".into());
                }
            }
        }
        let current = (track.state.clone(), track.reason.clone());
        if track.written.as_ref() != Some(&current) {
            let mut body = json!({"State": current.0});
            if let Some(reason) = &current.1 {
                body["Reason"] = json!(reason);
                body["Description"] = json!(track.description.clone().unwrap_or_default());
            }
            writes.push((format!("{}health", group.prefix), key, body));
            track.written = Some(current);
        }
    }
    // Drop documents of targets that are no longer registered.
    for (id, namespace) in &snapshot.health_namespaces {
        if !live.contains(id) {
            let namespace = namespace.clone();
            let id = id.clone();
            let db = db.clone();
            let _ = tokio::task::spawn_blocking(move || {
                db.transaction(|tx| {
                    tx.execute(
                        "DELETE FROM documents WHERE namespace=?1 AND id=?2",
                        params![namespace, id],
                    )?;
                    Ok(())
                })
            })
            .await;
        }
    }
    if !writes.is_empty() {
        let _ = tokio::task::spawn_blocking(move || {
            db.transaction(|tx| {
                for (namespace, id, body) in writes {
                    tx.execute(
                        "INSERT INTO documents(namespace,id,body) VALUES (?1,?2,?3) ON CONFLICT(namespace,id) DO UPDATE SET body=excluded.body",
                        params![namespace, id, body.to_string()],
                    )?;
                }
                Ok(())
            })
        })
        .await;
    }
}

async fn health_loop(store: Arc<Store>) {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .danger_accept_invalid_certs(true)
        .no_proxy()
        .user_agent("ELB-HealthChecker/2.0")
        .build()
        .expect("health check client");
    let mut tracks = HashMap::new();
    loop {
        health_tick(&store, &client, &mut tracks).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------------------------

/// Binds the proxy and starts it with the health checker. Returns the bound address.
pub async fn start(store: Arc<Store>, addr: SocketAddr) -> std::io::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let proxy = Arc::new(Proxy {
        store: store.clone(),
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(UPSTREAM_TIMEOUT)
            .build()
            .expect("proxy client"),
        round_robin: AtomicU64::new(0),
    });
    let app = Router::new().fallback(handle).with_state(proxy);
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    tokio::spawn(health_loop(store));
    Ok(bound)
}

#[cfg(test)]
mod tests;
