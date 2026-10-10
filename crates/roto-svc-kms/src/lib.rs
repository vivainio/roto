//! Lightweight symmetric KMS simulation. Ciphertexts contain plaintext; no cryptography.
#[allow(clippy::all)]
mod generated;
use base64::{Engine, engine::general_purpose::STANDARD};
use generated::*;
pub use generated::{OPERATIONS, Service, dispatch};
use roto_core::rusqlite::{OptionalExtension, Transaction, params};
use roto_core::store::{Db, Migration, Store};
use roto_core::{AwsError, RawRequest, RawResponse, RequestContext, ServiceHandler};
use roto_protocol::{FromJson, ToJson, json_error};
use serde_json::{Value, json};
use std::sync::Arc;
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "CREATE TABLE keys (account TEXT, region TEXT, id TEXT, metadata TEXT NOT NULL, PRIMARY KEY(account,region,id)); CREATE TABLE aliases (account TEXT, region TEXT, name TEXT, key_id TEXT NOT NULL, PRIMARY KEY(account,region,name));",
}];
pub struct Kms {
    db: Arc<Db>,
}
pub struct KmsHandler(pub Arc<Kms>);
impl KmsHandler {
    pub fn new(store: &Store) -> Result<Self, AwsError> {
        Ok(Self(Arc::new(Kms {
            db: store.db("kms", MIGRATIONS)?,
        })))
    }
}
impl ServiceHandler for KmsHandler {
    fn service(&self) -> &'static str {
        "kms"
    }
    fn handle(&self, ctx: &RequestContext, req: &RawRequest) -> Result<RawResponse, AwsError> {
        let result = (|| {
            let target = req
                .header("x-amz-target")
                .ok_or_else(|| error("MissingAuthenticationToken", "Missing X-Amz-Target"))?;
            let input: Value = serde_json::from_slice(&req.body)
                .map_err(|_| error("SerializationException", "Invalid JSON"))?;
            dispatch(
                &*self.0,
                ctx,
                target.rsplit('.').next().unwrap_or(target),
                &input,
            )
        })();
        Ok(result
            .unwrap_or_else(|e| json_error(generated::JSON_VERSION, &e, &ctx.request_id, false)))
    }
    fn reset(&self) -> Result<(), AwsError> {
        self.0.db.transaction(|tx| {
            tx.execute("DELETE FROM aliases", [])?;
            tx.execute("DELETE FROM keys", [])?;
            Ok(())
        })
    }
}
fn error(code: &str, msg: &str) -> AwsError {
    AwsError::sender(400, code, msg)
}
fn text<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}
fn context(v: &Value, k: &str) -> Value {
    v.get(k).cloned().unwrap_or_else(|| json!({}))
}
fn load(tx: &Transaction, ctx: &RequestContext, id: &str) -> Result<Value, AwsError> {
    let id = id
        .split(":alias/")
        .nth(1)
        .map(|s| format!("alias/{s}"))
        .unwrap_or_else(|| id.to_owned());
    let id = if id.starts_with("alias/") {
        tx.query_row(
            "SELECT key_id FROM aliases WHERE account=?1 AND region=?2 AND name=?3",
            params![ctx.account_id, ctx.region, id],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| error("NotFoundException", "Alias not found"))?
    } else {
        id.strip_prefix(&format!(
            "arn:aws:kms:{}:{}:key/",
            ctx.region, ctx.account_id
        ))
        .unwrap_or(&id)
        .to_owned()
    };
    let data: String = tx
        .query_row(
            "SELECT metadata FROM keys WHERE account=?1 AND region=?2 AND id=?3",
            params![ctx.account_id, ctx.region, id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| error("NotFoundException", "Key not found"))?;
    serde_json::from_str(&data).map_err(|_| error("InternalException", "Invalid stored key"))
}
fn usable(key: &Value) -> Result<(), AwsError> {
    if key["KeyState"] != "Enabled" {
        return Err(error("DisabledException", "Key is disabled"));
    }
    Ok(())
}
fn algorithm(v: &Value, k: &str) -> Result<(), AwsError> {
    if v.get(k).is_some() && v[k] != "SYMMETRIC_DEFAULT" {
        return Err(error(
            "ValidationException",
            "Only SYMMETRIC_DEFAULT is supported",
        ));
    }
    Ok(())
}
fn blob(v: &Value, k: &str) -> Result<Vec<u8>, AwsError> {
    STANDARD
        .decode(text(v, k))
        .map_err(|_| error("InvalidCiphertextException", "Invalid base64 blob"))
}
fn encrypt(key: &Value, plain: &[u8], ctx: Value) -> String {
    STANDARD.encode(serde_json::to_vec(&json!({"roto_kms":1,"key_id":key["Arn"],"context":ctx,"plaintext":STANDARD.encode(plain)})).unwrap())
}
fn decrypt(
    tx: &Transaction,
    ctx: &RequestContext,
    v: &Value,
    context_key: &str,
    key_field: &str,
) -> Result<(Value, Vec<u8>), AwsError> {
    let fail = || {
        error(
            "InvalidCiphertextException",
            "Invalid ciphertext or encryption context",
        )
    };
    let env: Value = serde_json::from_slice(&blob(v, "CiphertextBlob")?).map_err(|_| fail())?;
    if env["roto_kms"] != 1
        || !env["context"].is_object()
        || env["context"] != context(v, context_key)
    {
        return Err(fail());
    }
    let key = load(tx, ctx, env["key_id"].as_str().ok_or_else(fail)?)?;
    usable(&key)?;
    if let Some(id) = v.get(key_field)
        && load(tx, ctx, id.as_str().unwrap_or(""))?["Arn"] != key["Arn"]
    {
        return Err(error(
            "IncorrectKeyException",
            "Ciphertext belongs to another key",
        ));
    }
    let plain = STANDARD
        .decode(env["plaintext"].as_str().ok_or_else(fail)?)
        .map_err(|_| fail())?;
    Ok((key, plain))
}
impl Kms {
    fn call(&self, ctx: &RequestContext, op: &str, v: &Value) -> Result<Value, AwsError> {
        self.db.transaction(|tx| {
 match op {
 "CreateKey"=> {
  for (field,expected) in [("KeySpec","SYMMETRIC_DEFAULT"),("CustomerMasterKeySpec","SYMMETRIC_DEFAULT"),("KeyUsage","ENCRYPT_DECRYPT"),("Origin","AWS_KMS")] { if v.get(field).is_some() && v[field]!=expected { return Err(error("ValidationException","Only AWS_KMS symmetric encryption keys are supported")); } }
  if v["MultiRegion"]==true { return Err(error("ValidationException","Multi-region keys are unsupported")); }
  let id=uuid::Uuid::new_v4().to_string();
  let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64();
  let key=json!({"AWSAccountId":ctx.account_id,"KeyId":id,"Arn":format!("arn:aws:kms:{}:{}:key/{id}",ctx.region,ctx.account_id),"CreationDate":now,"Enabled":true,"KeyState":"Enabled","Description":text(v,"Description"),"KeyUsage":"ENCRYPT_DECRYPT","KeySpec":"SYMMETRIC_DEFAULT","CustomerMasterKeySpec":"SYMMETRIC_DEFAULT","Origin":"AWS_KMS","KeyManager":"CUSTOMER","EncryptionAlgorithms":["SYMMETRIC_DEFAULT"],"MultiRegion":false});
  tx.execute("INSERT INTO keys VALUES (?1,?2,?3,?4)",params![ctx.account_id,ctx.region,id,key.to_string()])?;
  Ok(json!({"KeyMetadata":key}))
 },
 "DescribeKey"=>Ok(json!({"KeyMetadata":load(tx,ctx,text(v,"KeyId"))?})),
 "EnableKey"|"DisableKey"=> { let mut key=load(tx,ctx,text(v,"KeyId"))?; let enabled=op=="EnableKey"; key["Enabled"]=json!(enabled); key["KeyState"]=json!(if enabled {"Enabled"} else {"Disabled"}); tx.execute("UPDATE keys SET metadata=?1 WHERE account=?2 AND region=?3 AND id=?4",params![key.to_string(),ctx.account_id,ctx.region,text(&key,"KeyId")])?; Ok(json!({})) },
 "CreateAlias"=> {
  let name=text(v,"AliasName"); if !name.starts_with("alias/") || name.starts_with("alias/aws/") || name.len()<=6 { return Err(error("ValidationException","Invalid alias name")); }
  let key=load(tx,ctx,text(v,"TargetKeyId"))?;
  let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM aliases WHERE account=?1 AND region=?2 AND name=?3)",params![ctx.account_id,ctx.region,name],|r|r.get(0))?;
  if exists { return Err(error("AlreadyExistsException","Alias already exists")); }
  tx.execute("INSERT INTO aliases VALUES (?1,?2,?3,?4)",params![ctx.account_id,ctx.region,name,text(&key,"KeyId")])?; Ok(json!({}))
 },
 "Encrypt"|"GenerateDataKey"|"GenerateDataKeyWithoutPlaintext"=> {
  algorithm(v,"EncryptionAlgorithm")?; let key=load(tx,ctx,text(v,"KeyId"))?; usable(&key)?;
  let plain=if op=="Encrypt" { let b=blob(v,"Plaintext")?; if b.is_empty() || b.len()>4096 { return Err(error("ValidationException","Plaintext must contain 1 to 4096 bytes")); } b } else {
   let length=match (v.get("KeySpec"),v.get("NumberOfBytes")) { (Some(s),None) if s=="AES_256"=>32, (Some(s),None) if s=="AES_128"=>16, (None,Some(n))=>n.as_u64().filter(|n|*n>0 && *n<=1024).ok_or_else(||error("ValidationException","Invalid NumberOfBytes"))? as usize, _=>return Err(error("ValidationException","Specify either AES_128/AES_256 KeySpec or NumberOfBytes")) };
   let mut bytes=vec![0;length]; getrandom::fill(&mut bytes).map_err(|_|error("InternalException","Random byte generation failed"))?; bytes
  };
  let mut result=json!({"KeyId":key["Arn"],"CiphertextBlob":encrypt(&key,&plain,context(v,"EncryptionContext"))});
  if op=="Encrypt" { result["EncryptionAlgorithm"]=json!("SYMMETRIC_DEFAULT"); } else if op=="GenerateDataKey" { result["Plaintext"]=json!(STANDARD.encode(&plain)); } Ok(result)
 },
 "Decrypt"=> { algorithm(v,"EncryptionAlgorithm")?; let (key,plain)=decrypt(tx,ctx,v,"EncryptionContext","KeyId")?; Ok(json!({"KeyId":key["Arn"],"Plaintext":STANDARD.encode(plain),"EncryptionAlgorithm":"SYMMETRIC_DEFAULT"})) },
 "ReEncrypt"=> { algorithm(v,"SourceEncryptionAlgorithm")?; algorithm(v,"DestinationEncryptionAlgorithm")?; let (source,plain)=decrypt(tx,ctx,v,"SourceEncryptionContext","SourceKeyId")?; let dest=load(tx,ctx,text(v,"DestinationKeyId"))?; usable(&dest)?; Ok(json!({"SourceKeyId":source["Arn"],"KeyId":dest["Arn"],"CiphertextBlob":encrypt(&dest,&plain,context(v,"DestinationEncryptionContext")),"SourceEncryptionAlgorithm":"SYMMETRIC_DEFAULT","DestinationEncryptionAlgorithm":"SYMMETRIC_DEFAULT"})) },
 _=>Err(AwsError::not_implemented("kms",op))
 }
 })
    }
}

pub const IMPLEMENTED: &[&str] = &[
    "CreateKey",
    "DescribeKey",
    "EnableKey",
    "DisableKey",
    "CreateAlias",
    "Encrypt",
    "Decrypt",
    "ReEncrypt",
    "GenerateDataKey",
    "GenerateDataKeyWithoutPlaintext",
];
impl Service for Kms {
    fn create_alias(
        &self,
        ctx: &RequestContext,
        input: CreateAliasRequest,
    ) -> Result<(), AwsError> {
        self.call(ctx, "CreateAlias", &input.to_json())?;
        Ok(())
    }
    fn create_key(
        &self,
        ctx: &RequestContext,
        input: CreateKeyRequest,
    ) -> Result<CreateKeyResponse, AwsError> {
        CreateKeyResponse::from_json(&self.call(ctx, "CreateKey", &input.to_json())?, "")
    }
    fn decrypt(
        &self,
        ctx: &RequestContext,
        input: DecryptRequest,
    ) -> Result<DecryptResponse, AwsError> {
        DecryptResponse::from_json(&self.call(ctx, "Decrypt", &input.to_json())?, "")
    }
    fn describe_key(
        &self,
        ctx: &RequestContext,
        input: DescribeKeyRequest,
    ) -> Result<DescribeKeyResponse, AwsError> {
        DescribeKeyResponse::from_json(&self.call(ctx, "DescribeKey", &input.to_json())?, "")
    }
    fn disable_key(&self, ctx: &RequestContext, input: DisableKeyRequest) -> Result<(), AwsError> {
        self.call(ctx, "DisableKey", &input.to_json())?;
        Ok(())
    }
    fn enable_key(&self, ctx: &RequestContext, input: EnableKeyRequest) -> Result<(), AwsError> {
        self.call(ctx, "EnableKey", &input.to_json())?;
        Ok(())
    }
    fn encrypt(
        &self,
        ctx: &RequestContext,
        input: EncryptRequest,
    ) -> Result<EncryptResponse, AwsError> {
        EncryptResponse::from_json(&self.call(ctx, "Encrypt", &input.to_json())?, "")
    }
    fn generate_data_key(
        &self,
        ctx: &RequestContext,
        input: GenerateDataKeyRequest,
    ) -> Result<GenerateDataKeyResponse, AwsError> {
        GenerateDataKeyResponse::from_json(
            &self.call(ctx, "GenerateDataKey", &input.to_json())?,
            "",
        )
    }
    fn generate_data_key_without_plaintext(
        &self,
        ctx: &RequestContext,
        input: GenerateDataKeyWithoutPlaintextRequest,
    ) -> Result<GenerateDataKeyWithoutPlaintextResponse, AwsError> {
        GenerateDataKeyWithoutPlaintextResponse::from_json(
            &self.call(ctx, "GenerateDataKeyWithoutPlaintext", &input.to_json())?,
            "",
        )
    }
    fn re_encrypt(
        &self,
        ctx: &RequestContext,
        input: ReEncryptRequest,
    ) -> Result<ReEncryptResponse, AwsError> {
        ReEncryptResponse::from_json(&self.call(ctx, "ReEncrypt", &input.to_json())?, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ctx() -> RequestContext {
        RequestContext {
            account_id: "123456789012".into(),
            region: "us-east-1".into(),
            access_key: None,
            request_id: "test".into(),
            base_url: "http://localhost:5070".into(),
        }
    }
    fn key(k: &Kms, c: &RequestContext) -> String {
        k.call(c, "CreateKey", &json!({})).unwrap()["KeyMetadata"]["KeyId"]
            .as_str()
            .unwrap()
            .into()
    }
    #[test]
    fn binary_roundtrip_context_and_state() {
        let store = Store::ephemeral();
        let k = KmsHandler::new(&store).unwrap().0;
        let c = ctx();
        let id = key(&k, &c);
        k.call(
            &c,
            "CreateAlias",
            &json!({"AliasName":"alias/test","TargetKeyId":id}),
        )
        .unwrap();
        let plain = STANDARD.encode([0, 255, 128, 42]);
        let enc=k.call(&c,"Encrypt",&json!({"KeyId":"alias/test","Plaintext":plain,"EncryptionContext":{"purpose":"test"}})).unwrap();
        let input =
            json!({"CiphertextBlob":enc["CiphertextBlob"],"EncryptionContext":{"purpose":"test"}});
        assert_eq!(k.call(&c, "Decrypt", &input).unwrap()["Plaintext"], plain);
        assert_eq!(
            k.call(
                &c,
                "Decrypt",
                &json!({"CiphertextBlob":enc["CiphertextBlob"]})
            )
            .unwrap_err()
            .code,
            "InvalidCiphertextException"
        );
        let mut wrong = input.clone();
        wrong["KeyId"] = json!(key(&k, &c));
        assert_eq!(
            k.call(&c, "Decrypt", &wrong).unwrap_err().code,
            "IncorrectKeyException"
        );
        let mut other = ctx();
        other.region = "us-west-2".into();
        assert_eq!(
            k.call(&other, "Decrypt", &input).unwrap_err().code,
            "NotFoundException"
        );
        let mut other = ctx();
        other.account_id = "000000000000".into();
        assert!(k.call(&other, "Decrypt", &input).is_err());
        k.call(&c, "DisableKey", &json!({"KeyId":id})).unwrap();
        assert_eq!(
            k.call(&c, "Decrypt", &input).unwrap_err().code,
            "DisabledException"
        );
        k.call(&c, "EnableKey", &json!({"KeyId":id})).unwrap();
        assert_eq!(k.call(&c, "Decrypt", &input).unwrap()["Plaintext"], plain);
    }
    #[test]
    fn data_keys_and_reencrypt() {
        let store = Store::ephemeral();
        let k = KmsHandler::new(&store).unwrap().0;
        let c = ctx();
        let id = key(&k, &c);
        let dest = key(&k, &c);
        for length in [1, 16, 32, 1024] {
            let data = k
                .call(
                    &c,
                    "GenerateDataKey",
                    &json!({"KeyId":id,"NumberOfBytes":length}),
                )
                .unwrap();
            assert_eq!(
                STANDARD
                    .decode(data["Plaintext"].as_str().unwrap())
                    .unwrap()
                    .len(),
                length
            );
            let again = k
                .call(
                    &c,
                    "GenerateDataKey",
                    &json!({"KeyId":id,"NumberOfBytes":32}),
                )
                .unwrap();
            if length == 32 {
                assert_ne!(data["Plaintext"], again["Plaintext"]);
            }
            let re=k.call(&c,"ReEncrypt",&json!({"CiphertextBlob":data["CiphertextBlob"],"DestinationKeyId":dest,"DestinationEncryptionContext":{"x":"y"}})).unwrap();
            assert_eq!(
                k.call(
                    &c,
                    "Decrypt",
                    &json!({"CiphertextBlob":re["CiphertextBlob"],"EncryptionContext":{"x":"y"}})
                )
                .unwrap()["Plaintext"],
                data["Plaintext"]
            );
        }
        let data = k
            .call(
                &c,
                "GenerateDataKeyWithoutPlaintext",
                &json!({"KeyId":id,"KeySpec":"AES_128"}),
            )
            .unwrap();
        assert!(data.get("Plaintext").is_none());
        assert_eq!(
            STANDARD
                .decode(
                    k.call(
                        &c,
                        "Decrypt",
                        &json!({"CiphertextBlob":data["CiphertextBlob"]})
                    )
                    .unwrap()["Plaintext"]
                        .as_str()
                        .unwrap()
                )
                .unwrap()
                .len(),
            16
        );
        for input in [
            json!({"KeyId":id}),
            json!({"KeyId":id,"NumberOfBytes":0}),
            json!({"KeyId":id,"NumberOfBytes":1025}),
            json!({"KeyId":id,"KeySpec":"AES_256","NumberOfBytes":32}),
        ] {
            assert!(k.call(&c, "GenerateDataKey", &input).is_err());
        }
    }
    #[test]
    fn malformed_and_unsupported() {
        let store = Store::ephemeral();
        let k = KmsHandler::new(&store).unwrap().0;
        let c = ctx();
        let id = key(&k, &c);
        for data in ["!", "", "e30=", "bm90IGpzb24="] {
            assert_eq!(
                k.call(&c, "Decrypt", &json!({"CiphertextBlob":data}))
                    .unwrap_err()
                    .code,
                "InvalidCiphertextException"
            );
        }
        assert!(
            k.call(
                &c,
                "Encrypt",
                &json!({"KeyId":id,"Plaintext":"AA==","EncryptionAlgorithm":"RSAES_OAEP_SHA_256"})
            )
            .is_err()
        );
        assert!(
            k.call(&c, "CreateKey", &json!({"KeySpec":"RSA_2048"}))
                .is_err()
        );
        let response =
            dispatch(&*k, &c, "Encrypt", &json!({"KeyId":id,"Plaintext":"AP8="})).unwrap();
        assert_eq!(response.status, 200);
    }
    #[test]
    fn persistence_and_reset() {
        let path = std::env::temp_dir().join(format!("roto-kms-test-{}", uuid::Uuid::new_v4()));
        let c = ctx();
        let ciphertext;
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let k = KmsHandler::new(&store).unwrap();
            let id = key(&k.0, &c);
            k.0.call(
                &c,
                "CreateAlias",
                &json!({"AliasName":"alias/persist","TargetKeyId":id}),
            )
            .unwrap();
            ciphertext =
                k.0.call(
                    &c,
                    "Encrypt",
                    &json!({"KeyId":"alias/persist","Plaintext":"AP8="}),
                )
                .unwrap()["CiphertextBlob"]
                    .clone();
        }
        {
            let store = Store::open(&path, Default::default()).unwrap();
            let k = KmsHandler::new(&store).unwrap();
            assert_eq!(
                k.0.call(&c, "Decrypt", &json!({"CiphertextBlob":ciphertext}))
                    .unwrap()["Plaintext"],
                "AP8="
            );
            assert!(
                k.0.call(&c, "DescribeKey", &json!({"KeyId":"alias/persist"}))
                    .is_ok()
            );
            k.reset().unwrap();
            assert!(
                k.0.call(&c, "Decrypt", &json!({"CiphertextBlob":ciphertext}))
                    .is_err()
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
