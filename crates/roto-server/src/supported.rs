//! The operations each service implements, as declared by `#[implements_operations]` on its
//! `impl Service` block. `roto.call`/`roto.aws` reject the rest, and the Luau definitions
//! type only these.
use roto_core::{ImplementsOperations, supported_operations};

/// Operations implemented by `service` (a `roto.aws` name; `eventbridge` is `events`), or
/// `None` for a service without a model.
pub(crate) fn operations(service: &str) -> Option<Vec<&'static str>> {
    macro_rules! ops {
        ($krate:ident, $ty:ty) => {
            supported_operations(
                $krate::OPERATIONS,
                <$ty as ImplementsOperations>::implemented_methods(),
            )
        };
    }
    Some(match service {
        "cloudformation" => ops!(
            roto_svc_cloudformation,
            roto_svc_cloudformation::CloudFormation
        ),
        "dynamodb" => ops!(roto_svc_dynamodb, roto_svc_dynamodb::DynamoDb),
        "events" | "eventbridge" => ops!(roto_svc_eventbridge, roto_svc_eventbridge::EventBridge),
        "iam" => ops!(roto_svc_iam, roto_svc_iam::Iam),
        "kinesis" => ops!(roto_svc_kinesis, roto_svc_kinesis::Kinesis),
        "kms" => ops!(roto_svc_kms, roto_svc_kms::Kms),
        "lambda" => ops!(roto_svc_lambda, roto_svc_lambda::Lambda),
        "logs" => ops!(roto_svc_logs, roto_svc_logs::CloudWatchLogs),
        "s3" => ops!(roto_svc_s3, roto_svc_s3::S3),
        "secretsmanager" => ops!(
            roto_svc_secretsmanager,
            roto_svc_secretsmanager::SecretsManager
        ),
        "sns" => ops!(roto_svc_sns, roto_svc_sns::Sns),
        "sqs" => ops!(roto_svc_sqs, roto_svc_sqs::Sqs),
        "ssm" => ops!(roto_svc_ssm, roto_svc_ssm::Ssm),
        "sts" => ops!(roto_svc_sts, roto_svc_sts::Sts),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    /// Every operation given a typed client is implemented by its service.
    #[test]
    fn typed_operations_are_implemented() {
        for (service, _) in crate::luau_stubs::MODELS {
            let implemented = super::operations(service).unwrap();
            for op in crate::luau_stubs::typed_operations(service) {
                assert!(
                    implemented.contains(&op.as_str()),
                    "{service} {op} is typed but its service does not implement it"
                );
            }
        }
    }
}
