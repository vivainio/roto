use super::*;
use axum::routing::get;
use roto_core::store::Store;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

#[test]
fn wildcards_match_like_alb_conditions() {
    assert!(wildcard("/v1/*", "/v1/orders/1", false));
    assert!(wildcard("*", "", false));
    assert!(wildcard("/a?c", "/abc", false));
    assert!(!wildcard("/a?c", "/ac", false));
    assert!(wildcard("*.example.com", "API.Example.com", true));
    assert!(!wildcard("*.example.com", "example.com", true));
    assert!(!wildcard("/v1/*", "/V1/x", false));
    assert!(wildcard("a*b*c", "aXXbYYc", false));
    assert!(!wildcard("a*b*c", "aXXbYY", false));
}

#[test]
fn cidr_matching() {
    assert!(cidr_contains("10.0.0.0/8", ip("10.200.1.2")));
    assert!(!cidr_contains("10.0.0.0/8", ip("11.0.0.1")));
    assert!(cidr_contains("192.168.1.7", ip("192.168.1.7")));
    assert!(cidr_contains("0.0.0.0/0", ip("8.8.8.8")));
    assert!(cidr_contains("::1/128", ip("::1")));
    assert!(!cidr_contains("10.0.0.0/8", ip("::1")));
}

#[test]
fn health_matcher_ranges_and_lists() {
    assert!(matcher_accepts("200", 200));
    assert!(!matcher_accepts("200", 204));
    assert!(matcher_accepts("200-299", 204));
    assert!(matcher_accepts("200,404", 404));
    assert!(matcher_accepts("200-204, 301", 301));
    assert!(!matcher_accepts("200-204", 500));
}

#[test]
fn host_headers_split_with_ports() {
    assert_eq!(
        split_host("Web-1.us-east-1.elb.localhost:8080"),
        ("web-1.us-east-1.elb.localhost".into(), Some(8080))
    );
    assert_eq!(split_host("localhost"), ("localhost".into(), None));
    assert_eq!(split_host("[::1]:80"), ("[::1]".into(), Some(80)));
}

#[test]
fn percent_decoding_tolerates_bad_escapes() {
    assert_eq!(percent_decode("a%20b+c"), "a b c");
    assert_eq!(percent_decode("100%"), "100%");
    assert_eq!(percent_decode("%zz"), "%zz");
    assert_eq!(percent_decode("%e2%82"), "\u{fffd}");
}

#[test]
fn redirect_templates_expand() {
    let r = Redirect {
        protocol: "HTTPS".into(),
        host: "#{host}".into(),
        port: "443".into(),
        path: "/#{path}".into(),
        query: "#{query}".into(),
        status: 301,
    };
    assert_eq!(
        redirect_location(&r, "http", "example.com", 80, "/a/b", "x=1"),
        "https://example.com:443/a/b?x=1"
    );
}

// ---------------------------------------------------------------------------------------------
// End to end against an in-memory service database
// ---------------------------------------------------------------------------------------------

const ACCOUNT: &str = "123456789012/us-east-1/elbv2/";

fn put(db: &Db, kind: &str, id: &str, body: Value) {
    db.transaction(|tx| {
        tx.execute(
            "INSERT INTO documents(namespace,id,body) VALUES (?1,?2,?3) ON CONFLICT(namespace,id) DO UPDATE SET body=excluded.body",
            params![format!("{ACCOUNT}{kind}"), id, body.to_string()],
        )?;
        Ok(())
    })
    .unwrap();
}

fn forward(group: &str) -> Value {
    json!([{"Type":"forward","Order":1,"ForwardConfig":{"TargetGroups":[{"TargetGroupArn":group,"Weight":1}]}}])
}

fn seed_balancer(db: &Db, listener_actions: Value) {
    put(
        db,
        "lb",
        "lb1",
        json!({
            "LoadBalancerArn":"lb1","LoadBalancerName":"web","DNSName":"web-1.us-east-1.elb.localhost","Type":"application"
        }),
    );
    put(
        db,
        "listener",
        "l1",
        json!({
            "ListenerArn":"l1","LoadBalancerArn":"lb1","Port":80,"Protocol":"HTTP","DefaultActions":listener_actions
        }),
    );
}

fn seed_group(db: &Db, arn: &str, ports: &[u16], path: &str) {
    let targets: Vec<Value> = ports
        .iter()
        .map(|p| json!({"Id":"127.0.0.1","Port":p}))
        .collect();
    put(
        db,
        "tg",
        arn,
        json!({
            "TargetGroupArn":arn,"TargetType":"ip","Protocol":"HTTP","Port":80,
            "HealthCheckProtocol":"HTTP","HealthCheckPort":"traffic-port","HealthCheckEnabled":true,
            "HealthCheckPath":path,"HealthCheckIntervalSeconds":0,"HealthCheckTimeoutSeconds":2,
            "HealthyThresholdCount":2,"UnhealthyThresholdCount":2,"Matcher":{"HttpCode":"200"},
            "_Targets":targets
        }),
    );
}

/// An upstream that names itself, echoes what the proxy added, and has a switchable health path.
async fn upstream(name: &'static str, healthy: Arc<std::sync::atomic::AtomicBool>) -> u16 {
    let app = Router::new()
        .route(
            "/healthz",
            get(move || {
                let healthy = healthy.clone();
                async move {
                    if healthy.load(Ordering::SeqCst) {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .fallback(move |request: Request<Body>| async move {
            let h = request.headers();
            let get = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_owned()
            };
            format!(
                "{name} {} {} proto={} port={} host={} xff={}",
                request.method(),
                request.uri(),
                get("x-forwarded-proto"),
                get("x-forwarded-port"),
                get("host"),
                get("x-forwarded-for"),
            )
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    port
}

async fn fixture() -> (Arc<Store>, Arc<Db>, SocketAddr) {
    let store = Arc::new(Store::ephemeral());
    let db = store.db(SERVICE, crate::lua_service::MIGRATIONS).unwrap();
    let addr = start(store.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    (store, db, addr)
}

async fn call(addr: SocketAddr, host: &str, path: &str) -> (u16, HeaderMap, String) {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .unwrap();
    let response = client
        .get(format!("http://{addr}{path}"))
        .header("host", host)
        .header("x-test", "yes")
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    (status, headers, response.text().await.unwrap())
}

const HOST: &str = "web-1.us-east-1.elb.localhost";

#[tokio::test]
async fn forwards_with_proxy_headers_and_preserves_the_host() {
    let (_store, db, addr) = fixture().await;
    let port = upstream("a", Arc::new(true.into())).await;
    seed_group(&db, "tg1", &[port], "/healthz");
    seed_balancer(&db, forward("tg1"));
    let (status, _, body) = call(addr, HOST, "/v1/items?x=1").await;
    assert_eq!(status, 200);
    assert!(
        body.starts_with("a GET /v1/items?x=1 proto=http port=80"),
        "{body}"
    );
    assert!(body.contains(&format!("host={HOST}")), "{body}");
    assert!(body.contains("xff=127.0.0.1"), "{body}");
}

#[tokio::test]
async fn rules_apply_in_priority_order_before_the_default() {
    let (_store, db, addr) = fixture().await;
    let port = upstream("a", Arc::new(true.into())).await;
    seed_group(&db, "tg1", &[port], "/healthz");
    seed_balancer(&db, forward("tg1"));
    let fixed = |code: &str| json!([{"Type":"fixed-response","Order":1,"FixedResponseConfig":{"StatusCode":code,"ContentType":"text/plain","MessageBody":"rule"}}]);
    put(
        &db,
        "rule",
        "r2",
        json!({"_ListenerArn":"l1","Priority":"20","Conditions":[
        {"Field":"path-pattern","PathPatternConfig":{"Values":["/v1/*"]}}],"Actions":fixed("202")}),
    );
    put(
        &db,
        "rule",
        "r1",
        json!({"_ListenerArn":"l1","Priority":"10","Conditions":[
        {"Field":"path-pattern","PathPatternConfig":{"Values":["/v1/*"]}},
        {"Field":"http-header","HttpHeaderConfig":{"HttpHeaderName":"X-Test","Values":["YES"]}},
        {"Field":"http-request-method","HttpRequestMethodConfig":{"Values":["GET"]}},
        {"Field":"query-string","QueryStringConfig":{"Values":[{"Key":"a","Value":"b*"}]}}],"Actions":fixed("201")}),
    );

    let (status, _, body) = call(addr, HOST, "/v1/x?a=bcd").await;
    assert_eq!((status, body.as_str()), (201, "rule"));
    // The query condition fails, so the next rule is the one that answers.
    assert_eq!(call(addr, HOST, "/v1/x?a=zzz").await.0, 202);
    // No rule matches, so the default forward runs.
    assert!(
        call(addr, HOST, "/other")
            .await
            .2
            .starts_with("a GET /other")
    );
}

#[tokio::test]
async fn redirects_and_selects_the_balancer_by_host() {
    let (_store, db, addr) = fixture().await;
    seed_balancer(
        &db,
        json!([{"Type":"redirect","Order":1,"RedirectConfig":{
        "Protocol":"HTTPS","Port":"443","Host":"#{host}","Path":"/#{path}","Query":"#{query}","StatusCode":"HTTP_301"}}]),
    );
    let (status, headers, _) = call(addr, "web-1.us-east-1.elb.localhost:8888", "/a?b=1").await;
    assert_eq!(status, 301);
    assert_eq!(
        headers["location"],
        "https://web-1.us-east-1.elb.localhost:443/a?b=1"
    );
    // The DNS label need not carry the region suffix, and a lone balancer answers plain localhost.
    assert_eq!(call(addr, "web-1.other.elb.localhost", "/").await.0, 301);
    assert_eq!(call(addr, "localhost", "/").await.0, 301);
    assert_eq!(
        call(addr, "nothing-9.us-east-1.elb.localhost", "/").await.0,
        404
    );
}

#[tokio::test]
async fn only_healthy_targets_get_traffic_and_all_do_when_none_are() {
    let (_store, db, addr) = fixture().await;
    let a = upstream("a", Arc::new(true.into())).await;
    let b = upstream("b", Arc::new(true.into())).await;
    seed_group(&db, "tg1", &[a, b], "/healthz");
    seed_balancer(&db, forward("tg1"));
    let key = |port: u16| format!("tg1|127.0.0.1:{port}");

    // Nothing is healthy yet, so both targets receive traffic.
    let mut seen = HashSet::new();
    for _ in 0..6 {
        seen.insert(call(addr, HOST, "/").await.2.chars().next().unwrap());
    }
    assert_eq!(seen.len(), 2);

    put(&db, "health", &key(a), json!({"State":"healthy"}));
    put(
        &db,
        "health",
        &key(b),
        json!({"State":"unhealthy","Reason":"Target.Timeout"}),
    );
    for _ in 0..4 {
        assert!(call(addr, HOST, "/").await.2.starts_with("a "));
    }
}

#[tokio::test]
async fn empty_and_missing_target_groups_answer_503() {
    let (_store, db, addr) = fixture().await;
    seed_group(&db, "tg1", &[], "/healthz");
    seed_balancer(&db, forward("tg1"));
    assert_eq!(call(addr, HOST, "/").await.0, 503);
    put(
        &db,
        "listener",
        "l1",
        json!({"ListenerArn":"l1","LoadBalancerArn":"lb1","Port":80,"Protocol":"HTTP","DefaultActions":forward("gone")}),
    );
    assert_eq!(call(addr, HOST, "/").await.0, 503);
}

#[tokio::test]
async fn unreachable_targets_answer_502() {
    let (_store, db, addr) = fixture().await;
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    seed_group(&db, "tg1", &[dead], "/healthz");
    seed_balancer(&db, forward("tg1"));
    assert_eq!(call(addr, HOST, "/").await.0, 502);
}

#[tokio::test]
async fn health_checks_move_targets_between_states_and_write_documents() {
    let store = Arc::new(Store::ephemeral());
    let db = store.db(SERVICE, crate::lua_service::MIGRATIONS).unwrap();
    let up = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let port = upstream("a", up.clone()).await;
    seed_group(&db, "tg1", &[port], "/healthz");
    seed_balancer(&db, forward("tg1"));
    let client = reqwest::Client::new();
    let mut tracks = HashMap::new();
    let state = |db: &Db| -> Option<(String, Option<String>)> {
        let snapshot = Snapshot::load(db).unwrap();
        snapshot
            .health
            .get(&format!("tg1|127.0.0.1:{port}"))
            .map(|h| (h.state.clone(), h.reason.clone()))
    };

    health_tick(&store, &client, &mut tracks).await;
    assert_eq!(
        state(&db),
        Some(("initial".into(), Some("Elb.InitialHealthChecking".into())))
    );
    health_tick(&store, &client, &mut tracks).await;
    assert_eq!(state(&db), Some(("healthy".into(), None)));

    up.store(false, Ordering::SeqCst);
    health_tick(&store, &client, &mut tracks).await;
    assert_eq!(
        state(&db).unwrap().0,
        "healthy",
        "one failure is below the threshold"
    );
    health_tick(&store, &client, &mut tracks).await;
    assert_eq!(
        state(&db),
        Some((
            "unhealthy".into(),
            Some("Target.ResponseCodeMismatch".into())
        ))
    );

    up.store(true, Ordering::SeqCst);
    health_tick(&store, &client, &mut tracks).await;
    health_tick(&store, &client, &mut tracks).await;
    assert_eq!(state(&db), Some(("healthy".into(), None)));

    // Deregistering removes the target's health document.
    seed_group(&db, "tg1", &[], "/healthz");
    health_tick(&store, &client, &mut tracks).await;
    assert_eq!(state(&db), None);
}

#[tokio::test]
async fn unused_groups_are_not_checked() {
    let store = Arc::new(Store::ephemeral());
    let db = store.db(SERVICE, crate::lua_service::MIGRATIONS).unwrap();
    let port = upstream("a", Arc::new(true.into())).await;
    seed_group(&db, "tg1", &[port], "/healthz");
    seed_balancer(&db, forward("some-other-group"));
    let mut tracks = HashMap::new();
    health_tick(&store, &reqwest::Client::new(), &mut tracks).await;
    assert!(Snapshot::load(&db).unwrap().health.is_empty());
}
