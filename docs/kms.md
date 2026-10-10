# KMS simulation

KMS supports `CreateKey`, `DescribeKey`, `ListKeys`, `CreateAlias`, `UpdateAlias`,
`DeleteAlias`, `ListAliases`, `EnableKey`, `DisableKey`, `UpdateKeyDescription`,
`Encrypt`, `Decrypt`, `ReEncrypt`, `GenerateDataKey`,
`GenerateDataKeyWithoutPlaintext`, `GenerateRandom`, key tags, the default key
policy, rotation status, and deletion scheduling through the AWS JSON 1.1 protocol.
Keys and aliases persist in SQLite and are isolated by account and region.
Key IDs, key ARNs, alias names and alias ARNs resolve to the same key.

Only AWS_KMS, SYMMETRIC_DEFAULT, ENCRYPT_DECRYPT keys are supported.
Ciphertext bytes contain a JSON envelope with `roto_kms: 1`, the key ARN,
encryption context, and base64 plaintext. The wire protocol additionally
base64-encodes these ciphertext bytes as required for AWS blob fields.
This is reversible encoding and provides no cryptographic protection or
integrity. Anyone can read or modify the envelope. There is no per-message
plaintext/ciphertext table.

Decrypt checks envelope version, key existence, enabled state, the supplied key
(if any), and exact encryption-context equality. ReEncrypt checks the source
context and rewraps the plaintext with the destination key and context.
Encrypt accepts 1–4096 plaintext bytes. Data keys use OS random bytes, with
AES_128/AES_256 or a NumberOfBytes value from 1 to 1024; exactly one length
selector is required. WithoutPlaintext omits plaintext from the response.

Asymmetric operations, multi-region/external keys, grants, automatic rotation,
and policy enforcement are unsupported. Other services' encryption settings
are not connected to KMS, although S3 stores and returns SSE-KMS key metadata.
Reset removes all keys and aliases, so
previous ciphertext can no longer be decrypted through the API.

Run `scripts/run-moto-tests.sh test_kms` to run the pinned Moto 5.0.11 suite;
73 known failures are recorded in `tests/moto/expected_failures/test_kms.txt`.
Native state and envelope checks are in `cargo test -p roto-svc-kms`.
