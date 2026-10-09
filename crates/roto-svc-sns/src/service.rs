use std::collections::BTreeMap;
use std::sync::Arc;

use roto_core::rusqlite::{OptionalExtension, Row, Transaction, params};
use roto_core::store::{Db, Store};
use roto_core::{AwsError, RequestContext};
use roto_protocol::Timestamp;
use roto_svc_sqs::{ExternalAttribute, Sqs};
use serde_json::{Value, json};

use crate::MIGRATIONS;
use crate::filter::{self, Attr};
use crate::generated::*;

pub struct Sns {
    db: Arc<Db>,
    sqs: Arc<Sqs>,
}

impl Sns {
    pub fn new(store: &Store, sqs: Arc<Sqs>) -> Result<Self, AwsError> {
        Ok(Self {
            db: store.db("sns", MIGRATIONS)?,
            sqs,
        })
    }

    pub fn reset(&self) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute("DELETE FROM subscriptions", [])?;
            tx.execute("DELETE FROM topics", [])?;
            Ok(())
        })
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn invalid(message: impl Into<String>) -> AwsError {
    AwsError::sender(400, "InvalidParameter", message)
}

fn not_found(what: &str) -> AwsError {
    AwsError::sender(404, "NotFound", format!("{what} does not exist"))
}

fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

type Attrs = BTreeMap<String, String>;

fn attrs_of(s: String) -> Attrs {
    serde_json::from_str(&s).unwrap_or_default()
}

fn attrs_json(a: &Attrs) -> String {
    serde_json::to_string(a).unwrap_or_else(|_| "{}".into())
}

struct Topic {
    arn: String,
    account_id: String,
    region: String,
    name: String,
    attributes: Attrs,
    tags: Vec<Tag>,
}

impl Topic {
    fn fifo(&self) -> bool {
        self.name.ends_with(".fifo")
    }
}

const TOPIC_COLS: &str = "arn, account_id, region, name, attributes, tags";

fn tags_from_json(text: &str) -> Vec<Tag> {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .map(|a| {
            a.iter()
                .filter_map(|x| {
                    Some(Tag {
                        key: x["Key"].as_str()?.to_string(),
                        value: x["Value"].as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn tags_json(tags: &[Tag]) -> String {
    Value::Array(
        tags.iter()
            .map(|t| json!({"Key": t.key, "Value": t.value}))
            .collect(),
    )
    .to_string()
}

fn topic_from_row(r: &Row) -> roto_core::rusqlite::Result<Topic> {
    Ok(Topic {
        arn: r.get(0)?,
        account_id: r.get(1)?,
        region: r.get(2)?,
        name: r.get(3)?,
        attributes: attrs_of(r.get(4)?),
        tags: tags_from_json(&r.get::<_, String>(5)?),
    })
}

fn find_topic(tx: &Transaction, arn: &str) -> Result<Option<Topic>, AwsError> {
    Ok(tx
        .query_row(
            &format!("SELECT {TOPIC_COLS} FROM topics WHERE arn = ?1"),
            params![arn],
            topic_from_row,
        )
        .optional()?)
}

fn require_topic(tx: &Transaction, arn: &str) -> Result<Topic, AwsError> {
    find_topic(tx, arn)?.ok_or_else(|| not_found("Topic"))
}

struct Sub {
    arn: String,
    topic_arn: String,
    account_id: String,
    protocol: String,
    endpoint: String,
    attributes: Attrs,
    confirmed: bool,
    token: Option<String>,
}

const SUB_COLS: &str =
    "arn, topic_arn, account_id, protocol, endpoint, attributes, confirmed, token";

fn sub_from_row(r: &Row) -> roto_core::rusqlite::Result<Sub> {
    Ok(Sub {
        arn: r.get(0)?,
        topic_arn: r.get(1)?,
        account_id: r.get(2)?,
        protocol: r.get(3)?,
        endpoint: r.get(4)?,
        attributes: attrs_of(r.get(5)?),
        confirmed: r.get::<_, i64>(6)? != 0,
        token: r.get(7)?,
    })
}

fn find_sub(tx: &Transaction, arn: &str) -> Result<Option<Sub>, AwsError> {
    Ok(tx
        .query_row(
            &format!("SELECT {SUB_COLS} FROM subscriptions WHERE arn = ?1"),
            params![arn],
            sub_from_row,
        )
        .optional()?)
}

fn subs_of(tx: &Transaction, topic_arn: Option<&str>) -> Result<Vec<Sub>, AwsError> {
    let mut stmt = match topic_arn {
        Some(_) => tx.prepare(&format!(
            "SELECT {SUB_COLS} FROM subscriptions WHERE topic_arn = ?1 ORDER BY seq"
        ))?,
        None => tx.prepare(&format!(
            "SELECT {SUB_COLS} FROM subscriptions WHERE ?1 IS NULL ORDER BY seq"
        ))?,
    };
    Ok(stmt
        .query_map(params![topic_arn], sub_from_row)?
        .collect::<Result<_, _>>()?)
}

fn page<T>(
    items: Vec<T>,
    token: &Option<String>,
    size: usize,
) -> Result<(Vec<T>, Option<String>), AwsError> {
    let start = match token.as_deref() {
        None | Some("") => 0,
        Some(t) => t
            .parse::<usize>()
            .map_err(|_| invalid("Invalid parameter: NextToken"))?,
    };
    let total = items.len();
    let out: Vec<T> = items.into_iter().skip(start).take(size).collect();
    let next = (start + out.len() < total).then(|| (start + out.len()).to_string());
    Ok((out, next))
}

fn default_policy(topic: &Topic) -> String {
    json!({
        "Version": "2008-10-17",
        "Id": "__default_policy_ID",
        "Statement": [{
            "Effect": "Allow",
            "Sid": "__default_statement_ID",
            "Principal": {"AWS": "*"},
            "Action": ["SNS:GetTopicAttributes", "SNS:SetTopicAttributes", "SNS:AddPermission", "SNS:RemovePermission",
                       "SNS:DeleteTopic", "SNS:Subscribe", "SNS:ListSubscriptionsByTopic", "SNS:Publish"],
            "Resource": topic.arn,
            "Condition": {"StringEquals": {"AWS:SourceOwner": topic.account_id}}
        }]
    })
    .to_string()
}

fn topic_attributes(tx: &Transaction, t: &Topic) -> Result<Attrs, AwsError> {
    let subs = subs_of(tx, Some(&t.arn))?;
    let confirmed = subs.iter().filter(|s| s.confirmed).count();
    let mut a: Attrs = BTreeMap::new();
    a.insert("TopicArn".into(), t.arn.clone());
    a.insert("Owner".into(), t.account_id.clone());
    a.insert("DisplayName".into(), String::new());
    a.insert("Policy".into(), default_policy(t));
    a.insert("SubscriptionsConfirmed".into(), confirmed.to_string());
    a.insert(
        "SubscriptionsPending".into(),
        (subs.len() - confirmed).to_string(),
    );
    a.insert("SubscriptionsDeleted".into(), "0".into());
    a.insert(
        "EffectiveDeliveryPolicy".into(),
        json!({"http": {"defaultHealthyRetryPolicy": {"minDelayTarget": 20, "maxDelayTarget": 20, "numRetries": 3,
            "numMaxDelayRetries": 0, "numNoDelayRetries": 0, "numMinDelayRetries": 0, "backoffFunction": "linear"},
            "disableSubscriptionOverrides": false}})
        .to_string(),
    );
    if t.fifo() {
        a.insert("FifoTopic".into(), "true".into());
        a.insert("ContentBasedDeduplication".into(), "false".into());
    }
    a.extend(t.attributes.clone());
    Ok(a)
}

fn sub_attributes(s: &Sub) -> Attrs {
    let mut a: Attrs = BTreeMap::new();
    a.insert("SubscriptionArn".into(), s.arn.clone());
    a.insert("TopicArn".into(), s.topic_arn.clone());
    a.insert("Owner".into(), s.account_id.clone());
    a.insert("Protocol".into(), s.protocol.clone());
    a.insert("Endpoint".into(), s.endpoint.clone());
    a.insert("ConfirmationWasAuthenticated".into(), "true".into());
    a.insert("PendingConfirmation".into(), (!s.confirmed).to_string());
    a.insert("RawMessageDelivery".into(), "false".into());
    a.extend(s.attributes.clone());
    a
}

const PROTOCOLS: &[&str] = &[
    "http",
    "https",
    "email",
    "email-json",
    "sms",
    "sqs",
    "application",
    "lambda",
    "firehose",
];

fn valid_topic_name(name: &str) -> bool {
    let base = name.strip_suffix(".fifo").unwrap_or(name);
    !base.is_empty()
        && name.len() <= 256
        && base
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn validate_filter_policy(policy: &str) -> Result<(), AwsError> {
    let v: Value = serde_json::from_str(policy)
        .map_err(|_| invalid("Invalid parameter: FilterPolicy: failed to parse JSON."))?;
    if !v.is_object() {
        return Err(invalid(
            "Invalid parameter: FilterPolicy: Filter policy must be a JSON object",
        ));
    }
    Ok(())
}

struct Delivery<'a> {
    topic: &'a Topic,
    message: &'a str,
    subject: &'a Option<String>,
    attributes: &'a BTreeMap<String, MessageAttributeValue>,
    structure_json: bool,
    group: Option<String>,
    dedup: Option<String>,
    message_id: String,
}

fn string_attr(a: &MessageAttributeValue) -> String {
    a.string_value
        .clone()
        .or_else(|| {
            a.binary_value
                .as_ref()
                .map(|b| roto_protocol::base64::encode(&b.0))
        })
        .unwrap_or_default()
}

fn message_for(d: &Delivery, protocol: &str) -> Result<String, AwsError> {
    if !d.structure_json {
        return Ok(d.message.to_string());
    }
    let parsed: Value = serde_json::from_str(d.message).map_err(|_| {
        invalid("Invalid parameter: Message Structure - JSON message body failed to parse")
    })?;
    let obj = parsed.as_object().ok_or_else(|| {
        invalid("Invalid parameter: Message Structure - JSON message body failed to parse")
    })?;
    obj.get(protocol)
        .or_else(|| obj.get("default"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            invalid("Invalid parameter: Message Structure - No default entry in JSON message body")
        })
}

impl Sns {
    fn fan_out(&self, tx: &Transaction, d: &Delivery) -> Result<(), AwsError> {
        for s in subs_of(tx, Some(&d.topic.arn))? {
            if !s.confirmed {
                continue;
            }
            let attrs: BTreeMap<String, Attr> = d
                .attributes
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        Attr {
                            data_type: v.data_type.clone(),
                            value: string_attr(v),
                        },
                    )
                })
                .collect();
            if let Some(policy) = s.attributes.get("FilterPolicy").filter(|p| !p.is_empty()) {
                let policy: Value = serde_json::from_str(policy).unwrap_or(Value::Null);
                let on_body = s.attributes.get("FilterPolicyScope").map(String::as_str)
                    == Some("MessageBody");
                let ok = if on_body {
                    let body = message_for(d, &s.protocol)
                        .ok()
                        .and_then(|m| serde_json::from_str::<Value>(&m).ok())
                        .unwrap_or(Value::Null);
                    filter::matches_body(&policy, &body)
                } else {
                    filter::matches_attributes(&policy, &attrs)
                };
                if !ok {
                    continue;
                }
            }
            if s.protocol != "sqs" {
                continue; // other transports are accepted but not delivered anywhere
            }
            let text = message_for(d, "sqs")?;
            let raw = s.attributes.get("RawMessageDelivery").map(String::as_str) == Some("true");
            let (body, ext): (String, BTreeMap<String, ExternalAttribute>) = if raw {
                let ext = d
                    .attributes
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            ExternalAttribute {
                                data_type: v.data_type.clone(),
                                string_value: v.string_value.clone(),
                                binary_value: v.binary_value.as_ref().map(|b| b.0.clone()),
                            },
                        )
                    })
                    .collect();
                (text, ext)
            } else {
                let region = &d.topic.region;
                let mut env = json!({
                    "Type": "Notification",
                    "MessageId": d.message_id,
                    "TopicArn": d.topic.arn,
                    "Message": text,
                    "Timestamp": Timestamp(now()).to_iso8601_millis(),
                    "SignatureVersion": "1",
                    "Signature": "EXAMPLElDMXvB8r9R83tGoNn0ecwd5UjllzsvSA==",
                    "SigningCertURL": format!("https://sns.{region}.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem"),
                    "UnsubscribeURL": format!("https://sns.{region}.amazonaws.com/?Action=Unsubscribe&SubscriptionArn={}", s.arn),
                });
                if let Some(subject) = d.subject {
                    env["Subject"] = json!(subject);
                }
                if !d.attributes.is_empty() {
                    let mut m = serde_json::Map::new();
                    for (k, v) in d.attributes {
                        m.insert(
                            k.clone(),
                            json!({"Type": v.data_type, "Value": string_attr(v)}),
                        );
                    }
                    env["MessageAttributes"] = Value::Object(m);
                }
                (env.to_string(), BTreeMap::new())
            };
            self.sqs
                .deliver(&s.endpoint, &body, &ext, d.group.clone(), d.dedup.clone())?;
        }
        Ok(())
    }

    /// `TopicArn` or `TargetArn` -> topic, with FIFO checks.
    fn publish_one(
        &self,
        tx: &Transaction,
        topic: &Topic,
        message: &str,
        subject: &Option<String>,
        attributes: &BTreeMap<String, MessageAttributeValue>,
        structure: &Option<String>,
        group: &Option<String>,
        dedup: &Option<String>,
    ) -> Result<PublishResponse, AwsError> {
        if message.is_empty() {
            return Err(invalid("Invalid parameter: Empty message"));
        }
        if message.len() > 262_144 {
            return Err(invalid("Invalid parameter: Message too long"));
        }
        if let Some(s) = subject {
            if s.len() > 100 {
                return Err(invalid("Subject must be less than 100 characters"));
            }
        }
        let structure_json = match structure.as_deref() {
            None => false,
            Some("json") => true,
            Some(other) => {
                return Err(invalid(format!(
                    "Invalid parameter: MessageStructure - No default entry in JSON message body: {other}"
                )));
            }
        };
        let (mut group_id, mut dedup_id) = (group.clone(), dedup.clone());
        if topic.fifo() {
            if group_id.is_none() {
                return Err(invalid(
                    "Invalid parameter: The MessageGroupId parameter is required for FIFO topics",
                ));
            }
            if dedup_id.is_none()
                && topic
                    .attributes
                    .get("ContentBasedDeduplication")
                    .map(String::as_str)
                    != Some("true")
            {
                return Err(invalid(
                    "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
            if dedup_id.is_none() {
                dedup_id = Some(roto_protocol::base64::encode(message.as_bytes()));
            }
        } else {
            group_id = None;
            dedup_id = None;
        }
        let message_id = uuid::Uuid::new_v4().to_string();
        let d = Delivery {
            topic,
            message,
            subject,
            attributes,
            structure_json,
            group: group_id.clone(),
            dedup: dedup_id,
            message_id: message_id.clone(),
        };
        self.fan_out(tx, &d)?;
        Ok(PublishResponse {
            message_id: Some(message_id),
            sequence_number: topic.fifo().then(|| format!("{:020}", now())),
        })
    }
}

impl Service for Sns {
    fn create_topic(
        &self,
        ctx: &RequestContext,
        i: CreateTopicInput,
    ) -> Result<CreateTopicResponse, AwsError> {
        if !valid_topic_name(&i.name) {
            return Err(invalid("Invalid parameter: Topic Name"));
        }
        let fifo_attr = i.attributes.get("FifoTopic").map(String::as_str) == Some("true");
        if fifo_attr != i.name.ends_with(".fifo") {
            return Err(invalid("Invalid parameter: Topic Name"));
        }
        let arn = format!(
            "arn:{}:sns:{}:{}:{}",
            partition(&ctx.region),
            ctx.region,
            ctx.account_id,
            i.name
        );
        self.db.transaction(|tx| {
            if let Some(existing) = find_topic(tx, &arn)? {
                for (k, v) in &i.attributes {
                    if existing.attributes.get(k).is_some_and(|e| e != v) {
                        return Err(invalid("Invalid parameter: Attributes Reason: Topic already exists with different attributes"));
                    }
                }
                return Ok(CreateTopicResponse { topic_arn: Some(existing.arn) });
            }
            let seq: i64 = tx.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM topics", [], |r| r.get(0))?;
            tx.execute(
                "INSERT INTO topics (arn, account_id, region, name, attributes, tags, created_at, seq) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![arn, ctx.account_id, ctx.region, i.name, attrs_json(&i.attributes), tags_json(&i.tags), now(), seq],
            )?;
            Ok(CreateTopicResponse { topic_arn: Some(arn.clone()) })
        })
    }

    fn delete_topic(&self, _ctx: &RequestContext, i: DeleteTopicInput) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            // Deleting a topic that does not exist succeeds.
            tx.execute("DELETE FROM topics WHERE arn = ?1", params![i.topic_arn])?;
            Ok(())
        })
    }

    fn list_topics(
        &self,
        ctx: &RequestContext,
        i: ListTopicsInput,
    ) -> Result<ListTopicsResponse, AwsError> {
        self.db.transaction(|tx| {
            let mut stmt = tx.prepare(
                "SELECT arn FROM topics WHERE account_id = ?1 AND region = ?2 ORDER BY seq",
            )?;
            let arns: Vec<String> = stmt
                .query_map(params![ctx.account_id, ctx.region], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            let (items, next_token) = page(arns, &i.next_token, 100)?;
            Ok(ListTopicsResponse {
                topics: items
                    .into_iter()
                    .map(|a| crate::generated::Topic { topic_arn: Some(a) })
                    .collect(),
                next_token,
            })
        })
    }

    fn get_topic_attributes(
        &self,
        _ctx: &RequestContext,
        i: GetTopicAttributesInput,
    ) -> Result<GetTopicAttributesResponse, AwsError> {
        self.db.transaction(|tx| {
            let t = require_topic(tx, &i.topic_arn)?;
            Ok(GetTopicAttributesResponse {
                attributes: topic_attributes(tx, &t)?,
            })
        })
    }

    fn set_topic_attributes(
        &self,
        _ctx: &RequestContext,
        i: SetTopicAttributesInput,
    ) -> Result<(), AwsError> {
        const SETTABLE: &[&str] = &[
            "Policy",
            "DisplayName",
            "DeliveryPolicy",
            "KmsMasterKeyId",
            "ContentBasedDeduplication",
            "SignatureVersion",
            "TracingConfig",
            "FifoThroughputScope",
            "ApplicationSuccessFeedbackRoleArn",
            "ApplicationSuccessFeedbackSampleRate",
            "ApplicationFailureFeedbackRoleArn",
            "HTTPSuccessFeedbackRoleArn",
            "HTTPSuccessFeedbackSampleRate",
            "HTTPFailureFeedbackRoleArn",
            "LambdaSuccessFeedbackRoleArn",
            "LambdaSuccessFeedbackSampleRate",
            "LambdaFailureFeedbackRoleArn",
            "SQSSuccessFeedbackRoleArn",
            "SQSSuccessFeedbackSampleRate",
            "SQSFailureFeedbackRoleArn",
            "FirehoseSuccessFeedbackRoleArn",
            "FirehoseFailureFeedbackRoleArn",
            "FirehoseSuccessFeedbackSampleRate",
        ];
        if !SETTABLE.contains(&i.attribute_name.as_str()) {
            return Err(invalid("Invalid parameter: AttributeName"));
        }
        self.db.transaction(|tx| {
            let mut t = require_topic(tx, &i.topic_arn)?;
            t.attributes.insert(
                i.attribute_name.clone(),
                i.attribute_value.clone().unwrap_or_default(),
            );
            tx.execute(
                "UPDATE topics SET attributes = ?1 WHERE arn = ?2",
                params![attrs_json(&t.attributes), t.arn],
            )?;
            Ok(())
        })
    }

    fn subscribe(
        &self,
        ctx: &RequestContext,
        i: SubscribeInput,
    ) -> Result<SubscribeResponse, AwsError> {
        if !PROTOCOLS.contains(&i.protocol.as_str()) {
            return Err(invalid(format!(
                "Invalid parameter: Amazon SNS does not support this protocol string: {}",
                i.protocol
            )));
        }
        let endpoint = i.endpoint.clone().unwrap_or_default();
        if endpoint.is_empty() && i.protocol != "application" {
            return Err(invalid("Invalid parameter: Endpoint"));
        }
        if i.protocol == "sqs" && !endpoint.starts_with("arn:") {
            return Err(invalid("Invalid parameter: SQS endpoint ARN"));
        }
        if matches!(i.protocol.as_str(), "http" | "https")
            && !endpoint.starts_with(&format!("{}://", i.protocol))
        {
            return Err(invalid(
                "Invalid parameter: Endpoint must match the specified protocol",
            ));
        }
        if let Some(fp) = i.attributes.get("FilterPolicy") {
            validate_filter_policy(fp)?;
        }
        self.db.transaction(|tx| {
            let topic = require_topic(tx, &i.topic_arn)?;
            if let Some(existing) = subs_of(tx, Some(&topic.arn))?.into_iter().find(|s| s.protocol == i.protocol && s.endpoint == endpoint) {
                return Ok(SubscribeResponse { subscription_arn: Some(existing.arn) });
            }
            let arn = format!("{}:{}", topic.arn, uuid::Uuid::new_v4());
            // HTTP(S) and email subscriptions wait for a confirmation token.
            let confirmed = !matches!(i.protocol.as_str(), "http" | "https" | "email" | "email-json");
            let token = (!confirmed).then(|| uuid::Uuid::new_v4().simple().to_string());
            tx.execute(
                "INSERT INTO subscriptions (arn, topic_arn, account_id, region, protocol, endpoint, attributes, confirmed, token)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![arn, topic.arn, ctx.account_id, ctx.region, i.protocol, endpoint, attrs_json(&i.attributes), i64::from(confirmed), token],
            )?;
            Ok(SubscribeResponse {
                subscription_arn: Some(if confirmed || i.return_subscription_arn == Some(true) { arn } else { "pending confirmation".into() }),
            })
        })
    }

    fn confirm_subscription(
        &self,
        _ctx: &RequestContext,
        i: ConfirmSubscriptionInput,
    ) -> Result<ConfirmSubscriptionResponse, AwsError> {
        self.db.transaction(|tx| {
            let topic = require_topic(tx, &i.topic_arn)?;
            let sub = subs_of(tx, Some(&topic.arn))?
                .into_iter()
                .find(|s| s.token.as_deref() == Some(i.token.as_str()))
                .ok_or_else(|| invalid("Invalid parameter: Token"))?;
            tx.execute(
                "UPDATE subscriptions SET confirmed = 1 WHERE arn = ?1",
                params![sub.arn],
            )?;
            Ok(ConfirmSubscriptionResponse {
                subscription_arn: Some(sub.arn),
            })
        })
    }

    fn unsubscribe(&self, _ctx: &RequestContext, i: UnsubscribeInput) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            tx.execute(
                "DELETE FROM subscriptions WHERE arn = ?1",
                params![i.subscription_arn],
            )?;
            Ok(())
        })
    }

    fn list_subscriptions(
        &self,
        ctx: &RequestContext,
        i: ListSubscriptionsInput,
    ) -> Result<ListSubscriptionsResponse, AwsError> {
        self.db.transaction(|tx| {
            let subs: Vec<Sub> = subs_of(tx, None)?
                .into_iter()
                .filter(|s| s.account_id == ctx.account_id)
                .collect();
            let (items, next_token) = page(subs, &i.next_token, 100)?;
            Ok(ListSubscriptionsResponse {
                subscriptions: items.iter().map(to_subscription).collect(),
                next_token,
            })
        })
    }

    fn list_subscriptions_by_topic(
        &self,
        _ctx: &RequestContext,
        i: ListSubscriptionsByTopicInput,
    ) -> Result<ListSubscriptionsByTopicResponse, AwsError> {
        self.db.transaction(|tx| {
            let topic = require_topic(tx, &i.topic_arn)?;
            let (items, next_token) = page(subs_of(tx, Some(&topic.arn))?, &i.next_token, 100)?;
            Ok(ListSubscriptionsByTopicResponse {
                subscriptions: items.iter().map(to_subscription).collect(),
                next_token,
            })
        })
    }

    fn get_subscription_attributes(
        &self,
        _ctx: &RequestContext,
        i: GetSubscriptionAttributesInput,
    ) -> Result<GetSubscriptionAttributesResponse, AwsError> {
        self.db.transaction(|tx| {
            let s = find_sub(tx, &i.subscription_arn)?.ok_or_else(|| not_found("Subscription"))?;
            Ok(GetSubscriptionAttributesResponse {
                attributes: sub_attributes(&s),
            })
        })
    }

    fn set_subscription_attributes(
        &self,
        _ctx: &RequestContext,
        i: SetSubscriptionAttributesInput,
    ) -> Result<(), AwsError> {
        const SETTABLE: &[&str] = &[
            "RawMessageDelivery",
            "DeliveryPolicy",
            "FilterPolicy",
            "FilterPolicyScope",
            "RedrivePolicy",
            "SubscriptionRoleArn",
        ];
        if !SETTABLE.contains(&i.attribute_name.as_str()) {
            return Err(invalid("Invalid parameter: AttributeName"));
        }
        let value = i.attribute_value.clone().unwrap_or_default();
        match i.attribute_name.as_str() {
            "FilterPolicy" if !value.is_empty() => validate_filter_policy(&value)?,
            "FilterPolicyScope"
                if !matches!(value.as_str(), "MessageAttributes" | "MessageBody") =>
            {
                return Err(invalid(
                    "Invalid parameter: FilterPolicyScope: Invalid value [x]. Please use either MessageBody or MessageAttributes",
                ));
            }
            "RawMessageDelivery" if !matches!(value.as_str(), "true" | "false") => {
                return Err(invalid(
                    "Invalid parameter: RawMessageDelivery: Invalid value",
                ));
            }
            _ => {}
        }
        self.db.transaction(|tx| {
            let mut s =
                find_sub(tx, &i.subscription_arn)?.ok_or_else(|| not_found("Subscription"))?;
            s.attributes.insert(i.attribute_name.clone(), value);
            tx.execute(
                "UPDATE subscriptions SET attributes = ?1 WHERE arn = ?2",
                params![attrs_json(&s.attributes), s.arn],
            )?;
            Ok(())
        })
    }

    fn publish(&self, _ctx: &RequestContext, i: PublishInput) -> Result<PublishResponse, AwsError> {
        if i.topic_arn.is_none() && i.target_arn.is_none() && i.phone_number.is_none() {
            return Err(invalid(
                "Invalid parameter: TopicArn or TargetArn Reason: no value for required parameter",
            ));
        }
        if i.phone_number.is_some() && i.topic_arn.is_none() && i.target_arn.is_none() {
            return Ok(PublishResponse {
                message_id: Some(uuid::Uuid::new_v4().to_string()),
                sequence_number: None,
            });
        }
        let arn = i
            .topic_arn
            .clone()
            .or(i.target_arn.clone())
            .unwrap_or_default();
        self.db.transaction(|tx| {
            let topic = find_topic(tx, &arn)?.ok_or_else(|| not_found("Topic"))?;
            self.publish_one(
                tx,
                &topic,
                &i.message,
                &i.subject,
                &i.message_attributes,
                &i.message_structure,
                &i.message_group_id,
                &i.message_deduplication_id,
            )
        })
    }

    fn publish_batch(
        &self,
        _ctx: &RequestContext,
        i: PublishBatchInput,
    ) -> Result<PublishBatchResponse, AwsError> {
        if i.publish_batch_request_entries.is_empty() {
            return Err(AwsError::sender(
                400,
                "EmptyBatchRequest",
                "The batch request doesn't contain any entries.",
            ));
        }
        if i.publish_batch_request_entries.len() > 10 {
            return Err(AwsError::sender(
                400,
                "TooManyEntriesInBatchRequest",
                "The batch request contains more entries than permissible.",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for e in &i.publish_batch_request_entries {
            if !seen.insert(&e.id) {
                return Err(AwsError::sender(
                    400,
                    "BatchEntryIdsNotDistinct",
                    "Two or more batch entries in the request have the same Id.",
                ));
            }
        }
        self.db.transaction(|tx| {
            let topic = require_topic(tx, &i.topic_arn)?;
            let mut out = PublishBatchResponse::default();
            for e in &i.publish_batch_request_entries {
                match self.publish_one(
                    tx,
                    &topic,
                    &e.message,
                    &e.subject,
                    &e.message_attributes,
                    &e.message_structure,
                    &e.message_group_id,
                    &e.message_deduplication_id,
                ) {
                    Ok(r) => out.successful.push(PublishBatchResultEntry {
                        id: Some(e.id.clone()),
                        message_id: r.message_id,
                        sequence_number: r.sequence_number,
                    }),
                    Err(err) => out.failed.push(BatchResultErrorEntry {
                        id: e.id.clone(),
                        code: err.code,
                        message: Some(err.message),
                        sender_fault: true,
                    }),
                }
            }
            Ok(out)
        })
    }

    fn tag_resource(
        &self,
        _ctx: &RequestContext,
        i: TagResourceRequest,
    ) -> Result<TagResourceResponse, AwsError> {
        self.db.transaction(|tx| {
            let mut t = find_topic(tx, &i.resource_arn)?.ok_or_else(|| {
                AwsError::sender(404, "ResourceNotFound", "Resource does not exist")
            })?;
            for tag in &i.tags {
                match t.tags.iter_mut().find(|x| x.key == tag.key) {
                    Some(x) => x.value = tag.value.clone(),
                    None => t.tags.push(tag.clone()),
                }
            }
            if t.tags.len() > 50 {
                return Err(AwsError::sender(
                    400,
                    "TagLimitExceeded",
                    "Could not complete request: tag quota of per resource exceeded",
                ));
            }
            tx.execute(
                "UPDATE topics SET tags = ?1 WHERE arn = ?2",
                params![tags_json(&t.tags), t.arn],
            )?;
            Ok(TagResourceResponse::default())
        })
    }

    fn untag_resource(
        &self,
        _ctx: &RequestContext,
        i: UntagResourceRequest,
    ) -> Result<UntagResourceResponse, AwsError> {
        self.db.transaction(|tx| {
            let mut t = find_topic(tx, &i.resource_arn)?.ok_or_else(|| {
                AwsError::sender(404, "ResourceNotFound", "Resource does not exist")
            })?;
            t.tags.retain(|x| !i.tag_keys.contains(&x.key));
            tx.execute(
                "UPDATE topics SET tags = ?1 WHERE arn = ?2",
                params![tags_json(&t.tags), t.arn],
            )?;
            Ok(UntagResourceResponse::default())
        })
    }

    fn list_tags_for_resource(
        &self,
        _ctx: &RequestContext,
        i: ListTagsForResourceRequest,
    ) -> Result<ListTagsForResourceResponse, AwsError> {
        self.db.transaction(|tx| {
            let t = find_topic(tx, &i.resource_arn)?.ok_or_else(|| {
                AwsError::sender(404, "ResourceNotFound", "Resource does not exist")
            })?;
            Ok(ListTagsForResourceResponse { tags: t.tags })
        })
    }

    fn add_permission(&self, _ctx: &RequestContext, i: AddPermissionInput) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut t = require_topic(tx, &i.topic_arn)?;
            let mut policy: Value = serde_json::from_str(&t.attributes.get("Policy").cloned().unwrap_or_else(|| default_policy(&t))).unwrap_or_default();
            let statements = policy["Statement"].as_array_mut().ok_or_else(|| AwsError::internal("bad policy"))?;
            if statements.iter().any(|s| s["Sid"] == i.label) {
                return Err(invalid("Invalid parameter: Statement already exists"));
            }
            statements.push(json!({
                "Sid": i.label,
                "Effect": "Allow",
                "Principal": {"AWS": i.aws_account_id.iter().map(|a| format!("arn:aws:iam::{a}:root")).collect::<Vec<_>>()},
                "Action": i.action_name.iter().map(|a| format!("SNS:{a}")).collect::<Vec<_>>(),
                "Resource": t.arn,
            }));
            t.attributes.insert("Policy".into(), policy.to_string());
            tx.execute("UPDATE topics SET attributes = ?1 WHERE arn = ?2", params![attrs_json(&t.attributes), t.arn])?;
            Ok(())
        })
    }

    fn remove_permission(
        &self,
        _ctx: &RequestContext,
        i: RemovePermissionInput,
    ) -> Result<(), AwsError> {
        self.db.transaction(|tx| {
            let mut t = require_topic(tx, &i.topic_arn)?;
            let mut policy: Value = serde_json::from_str(
                &t.attributes
                    .get("Policy")
                    .cloned()
                    .unwrap_or_else(|| default_policy(&t)),
            )
            .unwrap_or_default();
            if let Some(st) = policy["Statement"].as_array_mut() {
                st.retain(|s| s["Sid"] != i.label);
            }
            t.attributes.insert("Policy".into(), policy.to_string());
            tx.execute(
                "UPDATE topics SET attributes = ?1 WHERE arn = ?2",
                params![attrs_json(&t.attributes), t.arn],
            )?;
            Ok(())
        })
    }
}

fn to_subscription(s: &Sub) -> Subscription {
    Subscription {
        subscription_arn: Some(s.arn.clone()),
        owner: Some(s.account_id.clone()),
        protocol: Some(s.protocol.clone()),
        endpoint: Some(s.endpoint.clone()),
        topic_arn: Some(s.topic_arn.clone()),
    }
}
