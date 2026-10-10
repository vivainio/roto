//! Resource lifecycle delegates to the same wire handlers used by SDK clients.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler, ids};
use roto_protocol::{XmlWriter, restxml::parse_xml};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::template::{partition, text};
use crate::validation;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resource {
    pub logical_id: String,
    pub resource_type: String,
    pub physical_id: String,
    pub name: String,
    pub properties: Value,
    pub attributes: BTreeMap<String, Value>,
    pub subscriptions: Vec<String>,
    pub status: String,
    pub reason: Option<String>,
}

pub fn validate_properties(ty: &str, props: &Value) -> Result<(), AwsError> {
    let keys: &[&str] = match ty {
        "AWS::SQS::Queue" => &[
            "QueueName",
            "Tags",
            "VisibilityTimeout",
            "MessageRetentionPeriod",
            "DelaySeconds",
            "MaximumMessageSize",
            "ReceiveMessageWaitTimeSeconds",
            "RedrivePolicy",
            "RedriveAllowPolicy",
            "FifoQueue",
            "ContentBasedDeduplication",
            "DeduplicationScope",
            "FifoThroughputLimit",
            "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds",
            "SqsManagedSseEnabled",
        ],
        "AWS::SNS::Topic" => &[
            "TopicName",
            "Tags",
            "DisplayName",
            "FifoTopic",
            "ContentBasedDeduplication",
            "KmsMasterKeyId",
            "Subscription",
        ],
        "AWS::Kinesis::Stream" => &[
            "Name",
            "ShardCount",
            "RetentionPeriodHours",
            "Tags",
            "StreamModeDetails",
            "StreamEncryption",
        ],
        "AWS::S3::Bucket" => &[
            "BucketName",
            "Tags",
            "BucketEncryption",
            "VersioningConfiguration",
            "PublicAccessBlockConfiguration",
        ],
        "AWS::DynamoDB::Table" => &[
            "TableName",
            "Tags",
            "AttributeDefinitions",
            "KeySchema",
            "BillingMode",
            "ProvisionedThroughput",
            "GlobalSecondaryIndexes",
            "LocalSecondaryIndexes",
            "StreamSpecification",
            "SSESpecification",
            "TableClass",
            "DeletionProtectionEnabled",
            "TimeToLiveSpecification",
            "PointInTimeRecoverySpecification",
        ],
        _ => return Err(validation(format!("Unsupported resource type: {ty}"))),
    };
    let object = props
        .as_object()
        .ok_or_else(|| validation("Properties must be an object"))?;
    for key in object.keys() {
        if !keys.contains(&key.as_str()) {
            return Err(validation(format!("Unsupported property {ty}.{key}")));
        }
    }
    Ok(())
}

fn name_key(ty: &str) -> &'static str {
    match ty {
        "AWS::SQS::Queue" => "QueueName",
        "AWS::SNS::Topic" => "TopicName",
        "AWS::S3::Bucket" => "BucketName",
        "AWS::Kinesis::Stream" => "Name",
        _ => "TableName",
    }
}

pub fn replacement(old: &Resource, ty: &str, props: &Value) -> bool {
    if old.resource_type != ty {
        return true;
    }
    let key = name_key(ty);
    if old.properties.get(key) != props.get(key)
        && !(ty == "AWS::Kinesis::Stream" && props.get(key).is_none())
    {
        return true;
    }
    let immutable: &[&str] = match ty {
        "AWS::SQS::Queue" => &["FifoQueue"],
        "AWS::SNS::Topic" => &["FifoTopic"],
        "AWS::DynamoDB::Table" => &[
            "KeySchema",
            "AttributeDefinitions",
            "GlobalSecondaryIndexes",
            "LocalSecondaryIndexes",
        ],
        _ => &[],
    };
    immutable
        .iter()
        .any(|k| old.properties.get(*k) != props.get(*k))
}

pub struct Resources {
    handlers: HashMap<&'static str, Arc<dyn ServiceHandler>>,
}

impl Resources {
    pub fn new(handlers: HashMap<&'static str, Arc<dyn ServiceHandler>>) -> Self {
        Self { handlers }
    }

    fn request(
        &self,
        ctx: &RequestContext,
        service: &str,
        request: RawRequest,
    ) -> Result<RawResponse, AwsError> {
        let response = self
            .handlers
            .get(service)
            .ok_or_else(|| validation(format!("Service unavailable: {service}")))?
            .handle(ctx, &request)?;
        if response.status < 400 {
            return Ok(response);
        }
        let (code, message) = if let Ok(json) = serde_json::from_slice::<Value>(&response.body) {
            (
                json.get("__type")
                    .or_else(|| json.get("Code"))
                    .and_then(Value::as_str)
                    .unwrap_or("ResourceFailure")
                    .rsplit('#')
                    .next()
                    .unwrap_or("ResourceFailure")
                    .to_string(),
                json.get("message")
                    .or_else(|| json.get("Message"))
                    .and_then(Value::as_str)
                    .unwrap_or("Resource operation failed")
                    .to_string(),
            )
        } else if let Ok(doc) = parse_xml(&response.body) {
            let get = |key| {
                doc.descendants()
                    .find(|n| n.has_tag_name(key))
                    .and_then(|n| n.text())
                    .unwrap_or("")
                    .to_string()
            };
            (get("Code"), get("Message"))
        } else {
            (
                "ResourceFailure".into(),
                String::from_utf8_lossy(&response.body).into(),
            )
        };
        Err(AwsError::sender(response.status, code, message))
    }

    fn json(
        &self,
        ctx: &RequestContext,
        service: &str,
        op: &str,
        input: Value,
    ) -> Result<Value, AwsError> {
        let response = self.request(
            ctx,
            service,
            RawRequest {
                method: "POST".into(),
                path: "/".into(),
                headers: vec![("x-amz-target".into(), op.into())],
                body: serde_json::to_vec(&input).map_err(|e| AwsError::internal(e.to_string()))?,
                ..Default::default()
            },
        )?;
        serde_json::from_slice(&response.body).map_err(|e| AwsError::internal(e.to_string()))
    }

    fn sns(&self, ctx: &RequestContext, op: &str, input: Value) -> Result<Vec<u8>, AwsError> {
        let mut params = vec![("Action".into(), op.into())];
        for (key, value) in input
            .as_object()
            .ok_or_else(|| validation("SNS input must be an object"))?
        {
            if key == "Attributes" {
                for (index, (k, v)) in value
                    .as_object()
                    .ok_or_else(|| validation("Attributes must be an object"))?
                    .iter()
                    .enumerate()
                {
                    params.push((format!("Attributes.entry.{}.key", index + 1), k.clone()));
                    params.push((format!("Attributes.entry.{}.value", index + 1), text(v)?));
                }
            } else {
                flatten(key, value, &mut params)?;
            }
        }
        let body = params
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        Ok(self
            .request(
                ctx,
                "sns",
                RawRequest {
                    method: "POST".into(),
                    path: "/".into(),
                    body: body.into_bytes(),
                    ..Default::default()
                },
            )?
            .body)
    }

    fn s3(
        &self,
        ctx: &RequestContext,
        method: &str,
        name: &str,
        query: &str,
        body: String,
    ) -> Result<(), AwsError> {
        self.request(
            ctx,
            "s3",
            RawRequest {
                method: method.into(),
                path: format!("/{}", encode(name)),
                query: query.into(),
                body: body.into_bytes(),
                ..Default::default()
            },
        )?;
        Ok(())
    }

    /// Allocate only the base resource. The stack journals its identity before configuration.
    pub fn create(
        &self,
        ctx: &RequestContext,
        stack: &str,
        id: &str,
        ty: &str,
        props: Value,
    ) -> Result<Resource, AwsError> {
        validate_properties(ty, &props)?;
        let generated = format!(
            "{}-{id}-{}",
            stack.chars().take(30).collect::<String>(),
            &ids::request_id()[..12]
        );
        let mut name = props
            .get(name_key(ty))
            .map(text)
            .transpose()?
            .unwrap_or(generated);
        if ty == "AWS::S3::Bucket" && props.get("BucketName").is_none() {
            name = name.to_ascii_lowercase().replace('_', "-");
        }
        if props.get(name_key(ty)).is_none()
            && (props["FifoQueue"] == true || props["FifoTopic"] == true)
            && !name.ends_with(".fifo")
        {
            name.push_str(".fifo");
        }
        let mut attrs = BTreeMap::new();
        let part = partition(&ctx.region);
        let physical_id = match ty {
            "AWS::SQS::Queue" => {
                if self
                    .json(ctx, "sqs", "GetQueueUrl", json!({"QueueName":name}))
                    .is_ok()
                {
                    return Err(validation(format!("Queue {name} already exists")));
                }
                self.json(
                    ctx,
                    "sqs",
                    "CreateQueue",
                    json!({"QueueName": name, "Attributes": sqs_attributes(&props)?}),
                )?;
                attrs.insert(
                    "Arn".into(),
                    json!(format!(
                        "arn:{part}:sqs:{}:{}:{name}",
                        ctx.region, ctx.account_id
                    )),
                );
                attrs.insert("QueueName".into(), json!(name));
                format!(
                    "https://sqs.{}.amazonaws.com/{}/{name}",
                    ctx.region, ctx.account_id
                )
            }
            "AWS::SNS::Topic" => {
                let arn = format!("arn:{part}:sns:{}:{}:{name}", ctx.region, ctx.account_id);
                match self.sns(ctx, "GetTopicAttributes", json!({"TopicArn": arn})) {
                    Ok(_) => return Err(validation(format!("Topic {name} already exists"))),
                    Err(e) if e.code == "NotFound" => {}
                    Err(e) => return Err(e),
                }
                let body = self.sns(
                    ctx,
                    "CreateTopic",
                    json!({"Name":name, "Attributes":sns_attributes(&props)?}),
                )?;
                let arn = xml_text(&body, "TopicArn")?;
                attrs.insert("TopicName".into(), json!(name));
                attrs.insert("Arn".into(), json!(arn));
                arn
            }
            "AWS::S3::Bucket" => {
                if self.s3(ctx, "HEAD", &name, "", String::new()).is_ok() {
                    return Err(validation(format!("Bucket {name} already exists")));
                }
                let mut body = XmlWriter::new();
                if ctx.region != "us-east-1" {
                    body.open("CreateBucketConfiguration");
                    body.element("LocationConstraint", &ctx.region);
                    body.close("CreateBucketConfiguration");
                }
                self.s3(ctx, "PUT", &name, "", body.finish())?;
                attrs.insert("Arn".into(), json!(format!("arn:{part}:s3:::{name}")));
                attrs.insert(
                    "DomainName".into(),
                    json!(format!("{name}.s3.amazonaws.com")),
                );
                attrs.insert(
                    "DualStackDomainName".into(),
                    json!(format!("{name}.s3.dualstack.{}.amazonaws.com", ctx.region)),
                );
                attrs.insert(
                    "RegionalDomainName".into(),
                    json!(format!("{name}.s3.{}.amazonaws.com", ctx.region)),
                );
                attrs.insert(
                    "WebsiteURL".into(),
                    json!(format!(
                        "http://{name}.s3-website.{}.amazonaws.com",
                        ctx.region
                    )),
                );
                name.clone()
            }
            "AWS::Kinesis::Stream" => {
                let mut input = json!({"StreamName":name});
                input["StreamModeDetails"] = props
                    .get("StreamModeDetails")
                    .cloned()
                    .unwrap_or_else(|| json!({"StreamMode":"PROVISIONED"}));
                if input["StreamModeDetails"]["StreamMode"] != "ON_DEMAND" {
                    input["ShardCount"] = props.get("ShardCount").cloned().unwrap_or(json!(1));
                }
                self.json(ctx, "kinesis", "CreateStream", input)?;
                attrs.insert(
                    "Arn".into(),
                    json!(format!(
                        "arn:{part}:kinesis:{}:{}:stream/{name}",
                        ctx.region, ctx.account_id
                    )),
                );
                name.clone()
            }
            "AWS::DynamoDB::Table" => {
                let mut input = props.clone();
                let o = input.as_object_mut().unwrap();
                o.remove("TimeToLiveSpecification");
                o.remove("PointInTimeRecoverySpecification");
                o.insert("TableName".into(), json!(name));
                let result = self.json(ctx, "dynamodb", "CreateTable", input)?;
                attrs.insert("Arn".into(), result["TableDescription"]["TableArn"].clone());
                if let Some(arn) = result["TableDescription"].get("LatestStreamArn") {
                    attrs.insert("StreamArn".into(), arn.clone());
                }
                name.clone()
            }
            _ => return Err(validation(format!("Unsupported resource type: {ty}"))),
        };
        Ok(Resource {
            logical_id: id.into(),
            resource_type: ty.into(),
            physical_id,
            name,
            properties: props,
            attributes: attrs,
            subscriptions: Vec::new(),
            status: "CREATE_IN_PROGRESS".into(),
            reason: None,
        })
    }

    pub fn configure(
        &self,
        ctx: &RequestContext,
        resource: &mut Resource,
        props: &Value,
        updating: bool,
    ) -> Result<(), AwsError> {
        match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => {
                let mut attrs = sqs_attributes(props)?;
                let url = &resource.physical_id;
                if updating {
                    for key in sqs_attributes(&resource.properties)?.keys() {
                        if !attrs.contains_key(key) {
                            let default = match key.as_str() {
                                "VisibilityTimeout" => "30",
                                "MessageRetentionPeriod" => "345600",
                                "DelaySeconds" | "ReceiveMessageWaitTimeSeconds" => "0",
                                "MaximumMessageSize" => "262144",
                                "ContentBasedDeduplication" => "false",
                                "DeduplicationScope" => "queue",
                                "FifoThroughputLimit" => "perQueue",
                                "KmsDataKeyReusePeriodSeconds" => "300",
                                "RedrivePolicy" | "RedriveAllowPolicy" | "KmsMasterKeyId" => "",
                                "SqsManagedSseEnabled" => "true",
                                _ => {
                                    return Err(validation(format!(
                                        "Removing queue property {key} is not supported"
                                    )));
                                }
                            };
                            attrs.insert(key.clone(), default.into());
                        }
                    }
                }
                if updating && !attrs.is_empty() {
                    self.json(
                        ctx,
                        "sqs",
                        "SetQueueAttributes",
                        json!({"QueueUrl":url,"Attributes":attrs}),
                    )?;
                }
                let old = tag_map(&resource.properties)?;
                let tags = tag_map(props)?;
                let removed: Vec<_> = old.keys().filter(|k| !tags.contains_key(*k)).collect();
                if !removed.is_empty() {
                    self.json(
                        ctx,
                        "sqs",
                        "UntagQueue",
                        json!({"QueueUrl":url,"TagKeys":removed}),
                    )?;
                }
                if !tags.is_empty() {
                    self.json(ctx, "sqs", "TagQueue", json!({"QueueUrl":url,"Tags":tags}))?;
                }
            }
            "AWS::SNS::Topic" => {
                let mut attrs = sns_attributes(props)?;
                if updating {
                    for key in sns_attributes(&resource.properties)?.keys() {
                        if !attrs.contains_key(key) {
                            let default = match key.as_str() {
                                "DisplayName" | "KmsMasterKeyId" => "",
                                "ContentBasedDeduplication" => "false",
                                _ => {
                                    return Err(validation(format!(
                                        "Removing topic property {key} is not supported"
                                    )));
                                }
                            };
                            attrs.insert(key.clone(), default.into());
                        }
                    }
                }
                for (key, v) in attrs {
                    self.sns(ctx, "SetTopicAttributes", json!({"TopicArn":resource.physical_id,"AttributeName":key,"AttributeValue":v}))?;
                }
                let old = tag_map(&resource.properties)?;
                let tags = tag_map(props)?;
                let removed: Vec<_> = old.keys().filter(|k| !tags.contains_key(*k)).collect();
                if !removed.is_empty() {
                    self.sns(
                        ctx,
                        "UntagResource",
                        json!({"ResourceArn":resource.physical_id,"TagKeys":removed}),
                    )?;
                }
                if !tags.is_empty() {
                    self.sns(
                        ctx,
                        "TagResource",
                        json!({"ResourceArn":resource.physical_id,"Tags":tag_list(tags)}),
                    )?;
                }
                if !updating || resource.properties.get("Subscription") != props.get("Subscription")
                {
                    // Only subscriptions owned by this stack are reconciled.
                    while let Some(arn) = resource.subscriptions.last() {
                        self.sns(ctx, "Unsubscribe", json!({"SubscriptionArn":arn}))?;
                        resource.subscriptions.pop();
                    }
                    if let Some(subs) = props.get("Subscription") {
                        for sub in subs
                            .as_array()
                            .ok_or_else(|| validation("Subscription must be a list"))?
                        {
                            let body = self.sns(ctx, "Subscribe", json!({"TopicArn":resource.physical_id,"Protocol":sub["Protocol"],"Endpoint":sub["Endpoint"],"ReturnSubscriptionArn":true}))?;
                            resource
                                .subscriptions
                                .push(xml_text(&body, "SubscriptionArn")?);
                        }
                    }
                }
            }
            "AWS::S3::Bucket" => {
                if let Some(encryption) = props.get("BucketEncryption") {
                    let rules = encryption["ServerSideEncryptionConfiguration"]
                        .as_array()
                        .ok_or_else(|| {
                            validation(
                                "BucketEncryption requires ServerSideEncryptionConfiguration",
                            )
                        })?;
                    let mut w = XmlWriter::new();
                    w.open("ServerSideEncryptionConfiguration");
                    for rule in rules {
                        w.open("Rule");
                        if let Some(default) = rule.get("ServerSideEncryptionByDefault") {
                            write_xml(&mut w, "ApplyServerSideEncryptionByDefault", default)?;
                        }
                        if let Some(enabled) = rule.get("BucketKeyEnabled") {
                            write_xml(&mut w, "BucketKeyEnabled", enabled)?;
                        }
                        w.close("Rule");
                    }
                    w.close("ServerSideEncryptionConfiguration");
                    self.s3(ctx, "PUT", &resource.name, "encryption", w.finish())?;
                } else if updating && resource.properties.get("BucketEncryption").is_some() {
                    self.s3(ctx, "DELETE", &resource.name, "encryption", String::new())?;
                }
                for (key, query, root) in [
                    (
                        "VersioningConfiguration",
                        "versioning",
                        "VersioningConfiguration",
                    ),
                    (
                        "PublicAccessBlockConfiguration",
                        "publicAccessBlock",
                        "PublicAccessBlockConfiguration",
                    ),
                ] {
                    if let Some(value) = props.get(key) {
                        let mut w = XmlWriter::new();
                        write_xml(&mut w, root, value)?;
                        self.s3(ctx, "PUT", &resource.name, query, w.finish())?;
                    } else if updating && resource.properties.get(key).is_some() {
                        if key == "VersioningConfiguration" {
                            self.s3(ctx,"PUT",&resource.name,query,"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>".into())?;
                        } else {
                            self.s3(ctx, "DELETE", &resource.name, query, String::new())?;
                        }
                    }
                }
                let tags = tag_map(props)?;
                if !tags.is_empty() {
                    let mut w = XmlWriter::new();
                    w.open("Tagging");
                    w.open("TagSet");
                    for (k, v) in tags {
                        w.open("Tag");
                        w.element("Key", &k);
                        w.element("Value", &v);
                        w.close("Tag");
                    }
                    w.close("TagSet");
                    w.close("Tagging");
                    self.s3(ctx, "PUT", &resource.name, "tagging", w.finish())?;
                } else if updating && !tag_map(&resource.properties)?.is_empty() {
                    self.s3(ctx, "DELETE", &resource.name, "tagging", String::new())?;
                }
            }
            "AWS::Kinesis::Stream" => {
                let stream = &resource.name;
                let desc = self.json(
                    ctx,
                    "kinesis",
                    "DescribeStreamSummary",
                    json!({"StreamName":stream}),
                )?["StreamDescriptionSummary"]
                    .clone();
                if let Some(mode) = props.get("StreamModeDetails")
                    && mode != &desc["StreamModeDetails"]
                {
                    self.json(
                        ctx,
                        "kinesis",
                        "UpdateStreamMode",
                        json!({"StreamARN":resource.attributes["Arn"],"StreamModeDetails":mode}),
                    )?;
                }
                if updating
                    && let Some(count) = props.get("ShardCount")
                    && count != &desc["OpenShardCount"]
                {
                    self.json(ctx,"kinesis","UpdateShardCount",json!({"StreamName":stream,"TargetShardCount":count,"ScalingType":"UNIFORM_SCALING"}))?;
                }
                let retention = props
                    .get("RetentionPeriodHours")
                    .cloned()
                    .unwrap_or(json!(24));
                if retention != desc["RetentionPeriodHours"] {
                    let op = if retention.as_u64().unwrap_or(0)
                        > desc["RetentionPeriodHours"].as_u64().unwrap_or(24)
                    {
                        "IncreaseStreamRetentionPeriod"
                    } else {
                        "DecreaseStreamRetentionPeriod"
                    };
                    self.json(
                        ctx,
                        "kinesis",
                        op,
                        json!({"StreamName":stream,"RetentionPeriodHours":retention}),
                    )?;
                }
                let old = tag_map(&resource.properties)?;
                let tags = tag_map(props)?;
                let removed: Vec<_> = old.keys().filter(|key| !tags.contains_key(*key)).collect();
                if !removed.is_empty() {
                    self.json(
                        ctx,
                        "kinesis",
                        "RemoveTagsFromStream",
                        json!({"StreamName":stream,"TagKeys":removed}),
                    )?;
                }
                if !tags.is_empty() {
                    self.json(
                        ctx,
                        "kinesis",
                        "AddTagsToStream",
                        json!({"StreamName":stream,"Tags":tags}),
                    )?;
                }
                if let Some(encryption) = props.get("StreamEncryption") {
                    self.json(ctx,"kinesis","StartStreamEncryption",json!({"StreamName":stream,"EncryptionType":encryption["EncryptionType"],"KeyId":encryption["KeyId"]}))?;
                } else if updating && resource.properties.get("StreamEncryption").is_some() {
                    self.json(ctx,"kinesis","StopStreamEncryption",json!({"StreamName":stream,"EncryptionType":"KMS","KeyId":resource.properties["StreamEncryption"]["KeyId"]}))?;
                }
            }
            "AWS::DynamoDB::Table" => {
                if updating {
                    let mut input = json!({"TableName":resource.name});
                    for key in [
                        "BillingMode",
                        "ProvisionedThroughput",
                        "StreamSpecification",
                        "SSESpecification",
                        "TableClass",
                        "DeletionProtectionEnabled",
                    ] {
                        if resource.properties.get(key) != props.get(key) {
                            if let Some(v) = props.get(key) {
                                input[key] = v.clone();
                            } else {
                                return Err(validation(format!(
                                    "Removing DynamoDB property {key} is not supported"
                                )));
                            }
                        }
                    }
                    if input.as_object().unwrap().len() > 1 {
                        self.json(ctx, "dynamodb", "UpdateTable", input)?;
                    }
                    let old = tag_map(&resource.properties)?;
                    let tags = tag_map(props)?;
                    let removed: Vec<_> = old.keys().filter(|k| !tags.contains_key(*k)).collect();
                    if !removed.is_empty() {
                        self.json(
                            ctx,
                            "dynamodb",
                            "UntagResource",
                            json!({"ResourceArn":resource.attributes["Arn"],"TagKeys":removed}),
                        )?;
                    }
                    if !tags.is_empty() {
                        self.json(
                            ctx,
                            "dynamodb",
                            "TagResource",
                            json!({"ResourceArn":resource.attributes["Arn"],"Tags":tag_list(tags)}),
                        )?;
                    }
                }
                for (key, op, outkey) in [
                    (
                        "TimeToLiveSpecification",
                        "UpdateTimeToLive",
                        "TimeToLiveSpecification",
                    ),
                    (
                        "PointInTimeRecoverySpecification",
                        "UpdateContinuousBackups",
                        "PointInTimeRecoverySpecification",
                    ),
                ] {
                    if let Some(value) = props.get(key) {
                        self.json(
                            ctx,
                            "dynamodb",
                            op,
                            json!({"TableName":resource.name,outkey:value}),
                        )?;
                    } else if updating && resource.properties.get(key).is_some() {
                        return Err(validation(format!(
                            "Removing DynamoDB property {key} is not supported"
                        )));
                    }
                }
            }
            _ => return Err(validation("Unsupported resource type")),
        }
        resource.properties = props.clone();
        Ok(())
    }

    pub fn delete(&self, ctx: &RequestContext, resource: &Resource) -> Result<(), AwsError> {
        let result = match resource.resource_type.as_str() {
            "AWS::SQS::Queue" => self
                .json(
                    ctx,
                    "sqs",
                    "DeleteQueue",
                    json!({"QueueUrl":resource.physical_id}),
                )
                .map(|_| ()),
            "AWS::SNS::Topic" => self
                .sns(ctx, "DeleteTopic", json!({"TopicArn":resource.physical_id}))
                .map(|_| ()),
            "AWS::S3::Bucket" => self.s3(ctx, "DELETE", &resource.name, "", String::new()),
            "AWS::Kinesis::Stream" => self
                .json(
                    ctx,
                    "kinesis",
                    "DeleteStream",
                    json!({"StreamName":resource.name}),
                )
                .map(|_| ()),
            "AWS::DynamoDB::Table" => self
                .json(
                    ctx,
                    "dynamodb",
                    "DeleteTable",
                    json!({"TableName":resource.name}),
                )
                .map(|_| ()),
            _ => Err(validation("Unsupported resource type")),
        };
        match result {
            Err(e)
                if matches!(
                    e.code.as_str(),
                    "NoSuchBucket"
                        | "ResourceNotFoundException"
                        | "AWS.SimpleQueueService.NonExistentQueue"
                        | "QueueDoesNotExist"
                ) =>
            {
                Ok(())
            }
            other => other,
        }
    }
}

pub fn tag_map(props: &Value) -> Result<BTreeMap<String, String>, AwsError> {
    let mut result = BTreeMap::new();
    if let Some(tags) = props.get("Tags") {
        for tag in tags
            .as_array()
            .ok_or_else(|| validation("Tags must be a list"))?
        {
            let key = tag["Key"]
                .as_str()
                .ok_or_else(|| validation("Tag requires Key"))?;
            result.insert(key.into(), text(&tag["Value"])?);
        }
    }
    Ok(result)
}

pub fn tag_list(tags: BTreeMap<String, String>) -> Value {
    json!(
        tags.into_iter()
            .map(|(k, v)| json!({"Key":k,"Value":v}))
            .collect::<Vec<_>>()
    )
}

fn sqs_attributes(props: &Value) -> Result<BTreeMap<String, String>, AwsError> {
    props
        .as_object()
        .unwrap()
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "QueueName" | "Tags"))
        .map(|(k, v)| {
            Ok((
                k.clone(),
                if v.is_object() || v.is_array() {
                    v.to_string()
                } else {
                    text(v)?
                },
            ))
        })
        .collect()
}
fn sns_attributes(props: &Value) -> Result<BTreeMap<String, String>, AwsError> {
    props
        .as_object()
        .unwrap()
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "TopicName" | "Tags" | "Subscription"))
        .map(|(k, v)| Ok((k.clone(), text(v)?)))
        .collect()
}
fn flatten(key: &str, v: &Value, out: &mut Vec<(String, String)>) -> Result<(), AwsError> {
    match v {
        Value::Object(o) => {
            for (k, v) in o {
                flatten(&format!("{key}.{k}"), v, out)?;
            }
        }
        Value::Array(a) => {
            for (i, v) in a.iter().enumerate() {
                flatten(&format!("{key}.member.{}", i + 1), v, out)?;
            }
        }
        _ => out.push((key.into(), text(v)?)),
    }
    Ok(())
}
fn xml_text(body: &[u8], key: &str) -> Result<String, AwsError> {
    parse_xml(body)?
        .descendants()
        .find(|n| n.has_tag_name(key))
        .and_then(|n| n.text())
        .map(str::to_string)
        .ok_or_else(|| AwsError::internal(format!("Missing {key} in resource response")))
}
fn write_xml(w: &mut XmlWriter, key: &str, v: &Value) -> Result<(), AwsError> {
    match v {
        Value::Object(o) => {
            w.open(key);
            for (k, v) in o {
                write_xml(w, k, v)?;
            }
            w.close(key);
        }
        Value::Array(a) => {
            for v in a {
                write_xml(w, key, v)?;
            }
        }
        _ => w.element(key, &text(v)?),
    }
    Ok(())
}
fn encode(value: &str) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            write!(out, "%{byte:02X}").unwrap();
        }
    }
    out
}
