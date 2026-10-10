//! Persist events alongside S3 mutations, then hand them to Lambda after commit.
use crate::generated::NotificationConfiguration;
use crate::schema::*;
use crate::service::{BucketRow, Obj, S3};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use roto_core::{AwsError, RawResponse, RequestContext, ids};
use roto_protocol::{Timestamp, restxml::XmlRead};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn form_encode(key: &str) -> String {
    let mut out = String::new();
    for b in key.bytes() {
        match b {
            b' ' => out.push('+'),
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub(crate) fn record(
    tx: &mut SqliteConnection,
    ctx: &RequestContext,
    bucket: &BucketRow,
    key: &str,
    event: &str,
    object: Option<&Obj>,
) -> Result<(), AwsError> {
    let source: Option<String> = bucket_configs::table
        .filter(bucket_configs::bucket.eq(&(&bucket.name)))
        .filter(bucket_configs::kind.eq("notification"))
        .select(bucket_configs::body)
        .first::<String>(tx)
        .optional()?;
    let Some(source) = source else {
        return Ok(());
    };
    let doc = roto_protocol::restxml::parse_xml(source.as_bytes())?;
    let config = NotificationConfiguration::read_xml(doc.root_element())?;
    let event_type = format!("s3:{event}");
    for destination in config.lambda_function_configurations {
        if !destination.events.iter().any(|pattern| {
            pattern == &event_type
                || pattern
                    .strip_suffix('*')
                    .is_some_and(|p| event_type.starts_with(p))
        }) {
            continue;
        }
        if let Some(filter) = destination.filter.and_then(|f| f.key) {
            if !filter.filter_rules.iter().all(|r| {
                let value =
                    roto_protocol::restxml::percent_decode_form(r.value.as_deref().unwrap_or(""));
                match r.name.as_deref() {
                    Some("prefix") => key.starts_with(&value),
                    Some("suffix") => key.ends_with(&value),
                    _ => false,
                }
            }) {
                continue;
            }
        }
        let id = ids::request_id();
        let context = json!({"account_id":bucket.account_id,"region":bucket.region,"request_id":ctx.request_id,"base_url":ctx.base_url});
        let sequence: i64 = diesel::insert_into(notification_outbox::table)
            .values((
                notification_outbox::id.eq(&(id)),
                notification_outbox::target.eq(&(destination.lambda_function_arn)),
                notification_outbox::context.eq(&(context.to_string())),
                notification_outbox::event.eq("{}"),
            ))
            .returning(notification_outbox::seq)
            .get_result(tx)?;
        let mut obj = json!({"key":form_encode(key),"sequencer":format!("{sequence:016X}")});
        if let Some(o) = object {
            if event.starts_with("ObjectCreated:") {
                obj["size"] = json!(o.size);
                obj["eTag"] = json!(o.etag);
            }
            if o.version_id != "null" {
                obj["versionId"] = json!(o.version_id);
            }
        }
        let payload = json!({"Records":[{
            "eventVersion":"2.1","eventSource":"aws:s3","awsRegion":bucket.region,
            "eventTime":Timestamp::now().to_iso8601_millis(),"eventName":event,
            "userIdentity":{"principalId":ctx.account_id},"requestParameters":{"sourceIPAddress":"127.0.0.1"},
            "responseElements":{"x-amz-request-id":ctx.request_id,"x-amz-id-2":"roto"},
            "s3":{"s3SchemaVersion":"1.0","configurationId":destination.id.unwrap_or_default(),
                "bucket":{"name":bucket.name,"arn":format!("arn:aws:s3:::{}",bucket.name),"ownerIdentity":{"principalId":bucket.account_id}},"object":obj}
        }]});
        diesel::update(notification_outbox::table.filter(notification_outbox::id.eq(&(id))))
            .set(notification_outbox::event.eq(&(payload.to_string())))
            .execute(tx)?;
    }
    if config.event_bridge_configuration.is_some() {
        let id = ids::request_id();
        let target = format!(
            "arn:aws:events:{}:{}:event-bus/default",
            bucket.region, bucket.account_id
        );
        let context = json!({"account_id":bucket.account_id,"region":bucket.region,"request_id":ctx.request_id,"base_url":ctx.base_url});
        let sequence: i64 = diesel::insert_into(notification_outbox::table)
            .values((
                notification_outbox::id.eq(&(id)),
                notification_outbox::target.eq(&(target)),
                notification_outbox::context.eq(&(context.to_string())),
                notification_outbox::event.eq("{}"),
            ))
            .returning(notification_outbox::seq)
            .get_result(tx)?;
        let created = event.starts_with("ObjectCreated:");
        let mut obj = json!({"key":key,"sequencer":format!("{sequence:016X}")});
        if let Some(o) = object {
            if created {
                obj["size"] = json!(o.size);
                obj["etag"] = json!(o.etag);
            }
            if o.version_id != "null" {
                obj["version-id"] = json!(o.version_id);
            }
        }
        let reason = match event {
            "ObjectCreated:Put" => "PutObject",
            "ObjectCreated:Copy" => "CopyObject",
            "ObjectCreated:CompleteMultipartUpload" => "CompleteMultipartUpload",
            _ => "DeleteObject",
        };
        let mut detail = json!({"version":"0","bucket":{"name":bucket.name},"object":obj,"request-id":ctx.request_id,"requester":ctx.account_id,"source-ip-address":"127.0.0.1","reason":reason});
        if !created {
            detail["deletion-type"] = json!(if event.ends_with("DeleteMarkerCreated") {
                "Delete Marker Created"
            } else {
                "Permanently Deleted"
            });
        }
        let payload = json!({"version":"0","id":id,"source":"aws.s3","detail-type":if created {"Object Created"} else {"Object Deleted"},"account":bucket.account_id,"region":bucket.region,"time":Timestamp::now().to_iso8601_millis(),"resources":[format!("arn:aws:s3:::{}",bucket.name)],"detail":detail});
        diesel::update(notification_outbox::table.filter(notification_outbox::id.eq(&(id))))
            .set(notification_outbox::event.eq(&(payload.to_string())))
            .execute(tx)?;
    }
    Ok(())
}

pub(crate) fn start_worker(s3: &Arc<S3>, lambda: Arc<roto_svc_lambda::Lambda>) {
    start_worker_with_events(s3, lambda, None);
}

pub(crate) fn start_worker_with_events(
    s3: &Arc<S3>,
    lambda: Arc<roto_svc_lambda::Lambda>,
    events: Option<Arc<roto_svc_eventbridge::EventBridge>>,
) {
    let weak = Arc::downgrade(s3);
    std::thread::Builder::new()
        .name("roto-s3-events".into())
        .spawn(move || {
            while let Some(s3) = weak.upgrade() {
                let next = s3.db.read(|c| {
                    Ok(notification_outbox::table
                        .filter(notification_outbox::due.le(now()))
                        .filter(notification_outbox::attempts.lt(3))
                        .order(notification_outbox::seq)
                        .select((
                            notification_outbox::id,
                            notification_outbox::target,
                            notification_outbox::context,
                            notification_outbox::event,
                            notification_outbox::attempts,
                        ))
                        .first::<(String, String, String, String, i32)>(c)
                        .optional()?)
                });
                match next {
                    Ok(Some((id, target, context, event, attempts))) => {
                        let context: Value = serde_json::from_str(&context).unwrap();
                        let ctx = RequestContext {
                            account_id: context["account_id"].as_str().unwrap().into(),
                            region: context["region"].as_str().unwrap().into(),
                            request_id: context["request_id"].as_str().unwrap().into(),
                            base_url: context["base_url"].as_str().unwrap().into(),
                            access_key: None,
                        };
                        let payload = serde_json::from_str(&event).unwrap();
                        let result = if target.starts_with("arn:aws:events:") {
                            events
                                .as_ref()
                                .ok_or_else(|| {
                                    AwsError::sender(
                                        400,
                                        "ServiceUnavailable",
                                        "EventBridge delivery is unavailable",
                                    )
                                })
                                .and_then(|events| events.enqueue_event(&ctx, "default", payload))
                        } else {
                            lambda.enqueue(&ctx, &target, payload).map(|_| ())
                        };
                        let result = s3.db.transaction(|tx| {
                            match result {
                                Ok(_) => {
                                    diesel::delete(
                                        notification_outbox::table
                                            .filter(notification_outbox::id.eq(&(id))),
                                    )
                                    .execute(tx)?;
                                }
                                Err(e) => {
                                    eprintln!("S3 notification {id} to {target}: {e}");
                                    diesel::update(
                                        notification_outbox::table
                                            .filter(notification_outbox::id.eq(&(id))),
                                    )
                                    .set((
                                        notification_outbox::attempts.eq(&(attempts + 1)),
                                        notification_outbox::due
                                            .eq(&(now() + 1000 * i64::from(attempts + 1))),
                                        notification_outbox::error.eq(&(e.to_string())),
                                    ))
                                    .execute(tx)?;
                                }
                            }
                            Ok(())
                        });
                        if let Err(e) = result {
                            eprintln!("S3 notification persistence: {e}");
                        }
                    }
                    Ok(None) => {
                        drop(s3);
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => {
                        eprintln!("S3 notification queue: {e}");
                        drop(s3);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        })
        .expect("S3 notification worker");
}

pub(crate) fn history(s3: &S3, ctx: &RequestContext) -> Result<RawResponse, AwsError> {
    let rows=s3.db.read(|c| {
        let rows=notification_outbox::table.order(notification_outbox::seq.desc()).limit(100).select((notification_outbox::id,notification_outbox::target,notification_outbox::context,notification_outbox::event,notification_outbox::attempts,notification_outbox::error)).load::<(String,String,String,String,i32,Option<String>)>(c)?;
        let mut out=Vec::new();
        for row in rows {let (id,target,context,event,attempts,error)=row;let context:Value=serde_json::from_str(&context).unwrap();if context["account_id"]==ctx.account_id && context["region"]==ctx.region {out.push(json!({"id":id,"target":target,"event":serde_json::from_str::<Value>(&event).unwrap(),"attempts":attempts,"error":error,"state":if attempts>=3 {"failed"} else {"queued"}}));}}
        Ok(out)
    })?;
    Ok(RawResponse {
        status: 200,
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(&json!({"notifications":rows})).unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::S3Handler;
    use roto_core::store::{Store, StoreOptions};
    use roto_core::{RawRequest, ServiceHandler};
    use std::collections::BTreeMap;
    use std::time::Instant;

    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            request_id: "s3-request".into(),
            base_url: "http://localhost:5070".into(),
            access_key: None,
        }
    }
    fn call(
        handler: &S3Handler,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
        headers: Vec<(String, String)>,
    ) -> RawResponse {
        handler
            .handle(
                &ctx(),
                &RawRequest {
                    method: method.into(),
                    path: path.into(),
                    query: query.into(),
                    body: body.to_vec(),
                    headers,
                },
            )
            .unwrap()
    }
    fn events(handler: &S3Handler) -> Vec<Value> {
        handler
            .0
            .db
            .read(|c| {
                Ok(notification_outbox::table
                    .order(notification_outbox::seq)
                    .select(notification_outbox::event)
                    .load::<String>(c)?
                    .iter()
                    .map(|s| serde_json::from_str(s).unwrap())
                    .collect())
            })
            .unwrap()
    }

    #[test]
    fn filtered_events_commit_with_objects_and_resume_after_restart() {
        let dir = std::env::temp_dir().join(format!("roto-s3-notification-{}", ids::request_id()));
        let executors = roto_svc_lambda::Executors {
            functions: BTreeMap::from([(
                "processor".into(),
                roto_svc_lambda::Executor::Command {
                    command: vec!["sh".into(), "-c".into(), "cat".into()],
                    cwd: None,
                    env: BTreeMap::new(),
                },
            )]),
        };
        {
            let store = Store::open(&dir, StoreOptions::default()).unwrap();
            let lambda = roto_svc_lambda::LambdaHandler::new(&store, executors.clone()).unwrap();
            let response=lambda.handle(&ctx(),&RawRequest {method:"POST".into(),path:"/2015-03-31/functions".into(),body:br#"{"FunctionName":"processor","Role":"arn:aws:iam::123456789012:role/test","Code":{"ZipFile":""}}"#.to_vec(),..Default::default()}).unwrap();
            assert_eq!(response.status, 201);
            // Leave S3's worker stopped so the outbox can be tested across a restart.
            let s3 = S3Handler::new(&store).unwrap();
            assert_eq!(call(&s3, "PUT", "/uploads", "", b"", vec![]).status, 200);
            let config=br#"<NotificationConfiguration><CloudFunctionConfiguration><Id>filtered</Id><CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:processor</CloudFunction><Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>incoming/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.json</Value></FilterRule></S3Key></Filter></CloudFunctionConfiguration></NotificationConfiguration>"#;
            assert_eq!(
                call(&s3, "PUT", "/uploads", "notification", config, vec![]).status,
                200
            );
            assert_eq!(
                call(&s3, "PUT", "/uploads/elsewhere.json", "", b"skip", vec![]).status,
                200
            );
            assert_eq!(
                call(
                    &s3,
                    "PUT",
                    "/uploads/incoming/skip.txt",
                    "",
                    b"skip",
                    vec![]
                )
                .status,
                200
            );
            assert!(events(&s3).is_empty());
            assert_eq!(
                call(
                    &s3,
                    "PUT",
                    "/uploads/incoming/a%20b%2B.json",
                    "",
                    b"hello",
                    vec![]
                )
                .status,
                200
            );
            assert_eq!(
                call(
                    &s3,
                    "PUT",
                    "/uploads/incoming/a%20b%2B.json",
                    "",
                    b"failed",
                    vec![("if-none-match".into(), "*".into())]
                )
                .status,
                412
            );
            let notifications = events(&s3);
            assert_eq!(notifications.len(), 1);
            let record = &notifications[0]["Records"][0];
            assert_eq!(record["eventName"], "ObjectCreated:Put");
            assert_eq!(record["s3"]["object"]["key"], "incoming%2Fa+b%2B.json");
            assert_eq!(record["s3"]["object"]["size"], 5);
            assert_eq!(
                call(
                    &s3,
                    "PUT",
                    "/uploads/incoming/copied.json",
                    "",
                    b"",
                    vec![(
                        "x-amz-copy-source".into(),
                        "/uploads/incoming/a%20b%2B.json".into()
                    )]
                )
                .status,
                200
            );
            assert_eq!(
                call(
                    &s3,
                    "DELETE",
                    "/uploads/incoming/copied.json",
                    "",
                    b"",
                    vec![]
                )
                .status,
                204
            );
            let upload = call(
                &s3,
                "POST",
                "/uploads/incoming/multipart.json",
                "uploads",
                b"",
                vec![],
            );
            let xml = String::from_utf8(upload.body).unwrap();
            let doc = roxmltree::Document::parse(&xml).unwrap();
            let id = doc
                .descendants()
                .find(|n| n.has_tag_name("UploadId"))
                .unwrap()
                .text()
                .unwrap();
            let part = call(
                &s3,
                "PUT",
                "/uploads/incoming/multipart.json",
                &format!("uploadId={id}&partNumber=1"),
                b"part",
                vec![],
            );
            assert_eq!(part.status, 200, "{:?}", part);
            let etag = &part
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
                .unwrap()
                .1;
            let body = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
            );
            assert_eq!(
                call(
                    &s3,
                    "POST",
                    "/uploads/incoming/multipart.json",
                    &format!("uploadId={id}"),
                    body.as_bytes(),
                    vec![]
                )
                .status,
                200
            );
            let names: Vec<_> = events(&s3)
                .iter()
                .map(|e| e["Records"][0]["eventName"].as_str().unwrap().to_owned())
                .collect();
            assert_eq!(
                names,
                vec![
                    "ObjectCreated:Put",
                    "ObjectCreated:Copy",
                    "ObjectRemoved:Delete",
                    "ObjectCreated:CompleteMultipartUpload"
                ]
            );
        }
        let store = Store::open(&dir, StoreOptions::default()).unwrap();
        let lambda = roto_svc_lambda::LambdaHandler::new(&store, executors).unwrap();
        let s3 = S3Handler::with_lambda(&store, lambda.0.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let history = lambda.0.history(&ctx()).unwrap();
            let invocations = history["invocations"].as_array().unwrap();
            if invocations.len() == 4 && invocations.iter().all(|i| i["state"] == "succeeded") {
                break;
            }
            assert!(Instant::now() < deadline, "{history}");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(events(&s3).is_empty());
        drop(s3);
        drop(lambda);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
