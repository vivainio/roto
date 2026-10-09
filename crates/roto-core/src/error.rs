use std::fmt;

/// An error that is returned to the caller as an AWS-style error response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsError {
    pub status: u16,
    pub code: String,
    pub message: String,
    /// `true` for client faults (`Sender`), `false` for service faults (`Receiver`).
    pub sender: bool,
}

impl AwsError {
    pub fn sender(status: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            sender: true,
        }
    }

    pub fn receiver(status: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            sender: false,
        }
    }

    pub fn invalid_action(action: &str) -> Self {
        Self::sender(
            400,
            "InvalidAction",
            format!("Could not find operation {action} for this service"),
        )
    }

    pub fn missing_parameter(name: &str) -> Self {
        Self::sender(
            400,
            "MissingParameter",
            format!("The request must contain the parameter {name}."),
        )
    }

    pub fn invalid_parameter_value(message: impl Into<String>) -> Self {
        Self::sender(400, "InvalidParameterValue", message)
    }

    /// The operation exists in the AWS API but roto does not implement it yet.
    pub fn not_implemented(service: &str, operation: &str) -> Self {
        Self::receiver(
            501,
            "NotImplemented",
            format!("roto does not implement {service}:{operation} yet"),
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::receiver(500, "InternalFailure", message)
    }
}

impl fmt::Display for AwsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for AwsError {}

impl From<rusqlite::Error> for AwsError {
    fn from(e: rusqlite::Error) -> Self {
        Self::internal(format!("storage error: {e}"))
    }
}
