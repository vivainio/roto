//! SQS (JSON 1.0, query-compatible errors). Types and dispatch are generated from the botocore
//! model into `generated.rs`; queues and messages live in `sqs.db`.

#[allow(clippy::all)]
mod generated;
mod schema;
mod service;

use std::sync::Arc;

use roto_core::store::{Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::json_error;
use serde_json::Value;

pub use generated::{OPERATIONS, Service, dispatch};
pub use service::{ExternalAttribute, Sqs};

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: "
CREATE TABLE queues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    region TEXT NOT NULL,
    name TEXT NOT NULL,
    attributes TEXT NOT NULL DEFAULT '{}',
    tags TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    UNIQUE (account_id, region, name)
);
CREATE TABLE messages (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    body TEXT NOT NULL,
    md5 TEXT NOT NULL,
    attrs TEXT NOT NULL DEFAULT '{}',
    md5_attrs TEXT,
    sent_at INTEGER NOT NULL,
    visible_at INTEGER NOT NULL,
    receive_count INTEGER NOT NULL DEFAULT 0,
    first_received_at INTEGER,
    receipt_handle TEXT,
    group_id TEXT,
    dedup_id TEXT,
    sender_id TEXT NOT NULL
);
CREATE INDEX messages_visible ON messages (queue_id, visible_at);
CREATE INDEX messages_receipt ON messages (queue_id, receipt_handle);
",
    },
    Migration {
        version: 2,
        sql: "
ALTER TABLE messages ADD COLUMN trace_header TEXT;
-- Every receipt handle ever issued, so old handles keep working and deleted messages stay 'known'.
CREATE TABLE receipts (
    handle TEXT PRIMARY KEY,
    queue_id INTEGER NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    seq INTEGER REFERENCES messages(seq) ON DELETE SET NULL
);
CREATE INDEX receipts_seq ON receipts (seq);
",
    },
];

/// Operations with real behaviour; the rest answer `NotImplemented`. Feeds the coverage matrix.
pub const IMPLEMENTED: &[&str] = &[
    "CreateQueue",
    "DeleteQueue",
    "GetQueueUrl",
    "ListQueues",
    "GetQueueAttributes",
    "SetQueueAttributes",
    "SendMessage",
    "SendMessageBatch",
    "ReceiveMessage",
    "DeleteMessage",
    "DeleteMessageBatch",
    "ChangeMessageVisibility",
    "ChangeMessageVisibilityBatch",
    "PurgeQueue",
    "TagQueue",
    "UntagQueue",
    "ListQueueTags",
    "AddPermission",
    "RemovePermission",
];

pub struct SqsHandler(pub Arc<Sqs>);

impl SqsHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Sqs::new(store)?)))
    }
}

impl ServiceHandler for SqsHandler {
    fn service(&self) -> &'static str {
        "sqs"
    }

    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let fail = |e: AwsError| json_error(generated::JSON_VERSION, &e, &ctx.request_id, true);
        let Some(target) = req.header("x-amz-target") else {
            return Ok(fail(AwsError::sender(
                400,
                "MissingAuthenticationToken",
                "Missing X-Amz-Target header",
            )));
        };
        let operation = target.rsplit('.').next().unwrap_or(target);
        let body: Value = if req.body.is_empty() {
            Value::Object(Default::default())
        } else {
            match serde_json::from_slice(&req.body) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(fail(AwsError::sender(
                        400,
                        "SerializationException",
                        format!("invalid JSON: {e}"),
                    )));
                }
            }
        };
        let mut body = body;
        // moto distinguishes `AttributeNames: []` (error naming an empty attribute) from the
        // parameter being absent (no attributes returned); the generated types cannot.
        if operation == "GetQueueAttributes" && body["AttributeNames"] == serde_json::json!([]) {
            body["AttributeNames"] = serde_json::json!([""]);
        }
        Ok(dispatch(&*self.0, ctx, operation, &body).unwrap_or_else(fail))
    }

    fn reset(&self) -> Result<(), AwsError> {
        self.0.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "rid".into(),
            base_url: "http://localhost:5070".into(),
        }
    }

    fn call(h: &SqsHandler, op: &str, body: Value) -> (u16, Value) {
        let req = RawRequest {
            method: "POST".into(),
            headers: vec![("x-amz-target".into(), format!("AmazonSQS.{op}"))],
            body: body.to_string().into_bytes(),
            ..Default::default()
        };
        let r = h.handle(&ctx(), &req).unwrap();
        (r.status, serde_json::from_slice(&r.body).unwrap())
    }

    fn handler() -> SqsHandler {
        SqsHandler::new(&Store::ephemeral()).unwrap()
    }

    #[test]
    fn send_receive_delete_round_trip() {
        let h = handler();
        let (s, q) = call(&h, "CreateQueue", json!({"QueueName": "q"}));
        assert_eq!(s, 200);
        let url = q["QueueUrl"].as_str().unwrap().to_string();
        assert_eq!(url, "http://localhost:5070/123456789012/q");

        let (_, sent) = call(
            &h,
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": "hello"}),
        );
        assert_eq!(sent["MD5OfMessageBody"], "5d41402abc4b2a76b9719d911017c592");

        let (_, got) = call(
            &h,
            "ReceiveMessage",
            json!({"QueueUrl": url, "AttributeNames": ["All"]}),
        );
        let m = &got["Messages"][0];
        assert_eq!(m["Body"], "hello");
        assert_eq!(m["Attributes"]["ApproximateReceiveCount"], "1");

        // In flight: not visible to another receive.
        let (_, again) = call(&h, "ReceiveMessage", json!({"QueueUrl": url}));
        assert!(again.get("Messages").is_none());

        call(
            &h,
            "DeleteMessage",
            json!({"QueueUrl": url, "ReceiptHandle": m["ReceiptHandle"]}),
        );
        let (_, attrs) = call(
            &h,
            "GetQueueAttributes",
            json!({"QueueUrl": url, "AttributeNames": ["All"]}),
        );
        assert_eq!(attrs["Attributes"]["ApproximateNumberOfMessages"], "0");
        assert_eq!(
            attrs["Attributes"]["ApproximateNumberOfMessagesNotVisible"],
            "0"
        );
    }

    #[test]
    fn visibility_timeout_zero_makes_message_available_again() {
        let h = handler();
        let (_, q) = call(&h, "CreateQueue", json!({"QueueName": "q"}));
        let url = q["QueueUrl"].clone();
        call(
            &h,
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": "x"}),
        );
        for expected in ["1", "2"] {
            let (_, got) = call(
                &h,
                "ReceiveMessage",
                json!({"QueueUrl": url, "VisibilityTimeout": 0, "AttributeNames": ["ApproximateReceiveCount"]}),
            );
            assert_eq!(
                got["Messages"][0]["Attributes"]["ApproximateReceiveCount"],
                expected
            );
        }
    }

    #[test]
    fn missing_queue_uses_legacy_query_error_code() {
        let h = handler();
        let (s, e) = call(&h, "GetQueueUrl", json!({"QueueName": "nope"}));
        assert_eq!(s, 400);
        assert_eq!(e["__type"], "AWS.SimpleQueueService.NonExistentQueue");
    }

    #[test]
    fn create_queue_is_idempotent_but_rejects_different_attributes() {
        let h = handler();
        let a = json!({"QueueName": "q", "Attributes": {"VisibilityTimeout": "10"}});
        assert_eq!(call(&h, "CreateQueue", a.clone()).0, 200);
        assert_eq!(call(&h, "CreateQueue", a).0, 200);
        let (s, e) = call(
            &h,
            "CreateQueue",
            json!({"QueueName": "q", "Attributes": {"VisibilityTimeout": "11"}}),
        );
        assert_eq!(
            (s, e["__type"].as_str().unwrap()),
            (400, "QueueAlreadyExists")
        );
    }

    #[test]
    fn dead_letter_queue_redrive() {
        let h = handler();
        let (_, dlq) = call(&h, "CreateQueue", json!({"QueueName": "dlq"}));
        let policy = json!({"deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq", "maxReceiveCount": 1}).to_string();
        let (_, q) = call(
            &h,
            "CreateQueue",
            json!({"QueueName": "src", "Attributes": {"RedrivePolicy": policy}}),
        );
        let (src, dlq) = (q["QueueUrl"].clone(), dlq["QueueUrl"].clone());
        call(
            &h,
            "SendMessage",
            json!({"QueueUrl": src, "MessageBody": "poison"}),
        );
        call(
            &h,
            "ReceiveMessage",
            json!({"QueueUrl": src, "VisibilityTimeout": 0}),
        );
        let (_, none) = call(&h, "ReceiveMessage", json!({"QueueUrl": src}));
        assert!(none.get("Messages").is_none());
        let (_, moved) = call(&h, "ReceiveMessage", json!({"QueueUrl": dlq}));
        assert_eq!(moved["Messages"][0]["Body"], "poison");
    }

    #[test]
    fn fifo_requires_group_and_dedupes() {
        let h = handler();
        let (_, q) = call(
            &h,
            "CreateQueue",
            json!({"QueueName": "q.fifo", "Attributes": {"FifoQueue": "true"}}),
        );
        let url = q["QueueUrl"].clone();
        let (s, _) = call(
            &h,
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": "a", "MessageDeduplicationId": "1"}),
        );
        assert_eq!(s, 400);
        let m = json!({"QueueUrl": url, "MessageBody": "a", "MessageDeduplicationId": "1", "MessageGroupId": "g"});
        let (_, first) = call(&h, "SendMessage", m.clone());
        let (_, second) = call(&h, "SendMessage", m);
        assert_eq!(first["MessageId"], second["MessageId"]);
    }

    #[test]
    fn batch_validation() {
        let h = handler();
        let (_, q) = call(&h, "CreateQueue", json!({"QueueName": "q"}));
        let url = q["QueueUrl"].clone();
        let (_, e) = call(
            &h,
            "SendMessageBatch",
            json!({"QueueUrl": url, "Entries": [
            {"Id": "a", "MessageBody": "1"}, {"Id": "a", "MessageBody": "2"}]}),
        );
        assert_eq!(e["__type"], "BatchEntryIdsNotDistinct");
        let (s, ok) = call(
            &h,
            "SendMessageBatch",
            json!({"QueueUrl": url, "Entries": [
            {"Id": "a", "MessageBody": "1"}, {"Id": "b", "MessageBody": "2"}]}),
        );
        assert_eq!(s, 200);
        assert_eq!(ok["Successful"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn message_attribute_digest_matches_aws_example() {
        // Reference value computed with moto 5.0.11 (`Message.attribute_md5`) for the same single attribute.
        let attrs = BTreeMap::from([(
            "test_attribute_name".to_string(),
            generated::MessageAttributeValue {
                data_type: "String".into(),
                string_value: Some("test_attribute_value".into()),
                ..Default::default()
            },
        )]);
        assert_eq!(
            service::md5_of_attributes(&attrs).unwrap(),
            "13d73e0028ec3fd86f5c6ebb7a8d7fde"
        );
    }
}
