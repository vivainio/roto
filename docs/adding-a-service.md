# Adding a service

A Rust service is one crate, one model file and a few registration lines. Read
[Service protocols](service-protocols.md) first to pick the protocol your
model uses. For a small worked example, see the CRUD service section in
[Development](development.md) (`roto-svc-logs`).

Services can also be written in Luau without a Rust crate; see
[Luau scripting guide](lua.md) and the [services plan](https://github.com/vivainio/roto/blob/main/lua-services-plan.md).

## Checklist

1. **Model.** Copy the botocore `service-2.json` to `models/<name>/`. It drives
   the generated types and, for `roto.call`, the client codec.
2. **Codegen.** Add `<name>` to the loop in `scripts/gen.sh`, then run it. This
   writes `crates/roto-svc-<name>/src/generated.rs` with the operation types,
   (de)serialisation and the dispatcher.
3. **Crate.** Create `crates/roto-svc-<name>/` (copy `roto-svc-logs`): `lib.rs`
   with the handler and its `ServiceHandler` impl, `service.rs` with the
   behaviour, `schema.rs` with the Diesel tables and migration. Add it to the
   workspace `Cargo.toml` and to `crates/roto-server/Cargo.toml`.
4. **Handler.** `service()` returns the SigV4 signing name, for example `logs`.
   That string is the key everything else uses: request routing, hooks,
   `roto.wire` and `roto.call`. Override `reset`, `operation_name`,
   `claims_unsigned` and `resolve_account` only when the service needs them.
5. **Register.** Construct the handler in `crates/roto-server/src/main.rs` and
   add it to the `handlers` list. Services that call other services (Lambda to
   SQS, SNS to SQS) take an `Arc` to them in the constructor.
6. **`roto.call`.** In `crates/roto-server/src/aws_client.rs`:
   - XML protocols (`rest-xml`, `query`): add `("<name>", include_str!(...))` to
     `MODELS`.
   - `json` protocols: add the name to `JSON_SERVICES`.
   - `rest-json`: not supported by `roto.call`; use `roto.wire`.
7. **Inspection UI (optional).** List the service's tables in
   `crates/roto-server/src/inspection.rs`.
8. **Tests.** Add Rust tests in the crate and a `setup.rs` test if the service
   is reachable from `roto.call`. For Moto parity add a baseline under
   `tests/moto/expected_failures/test_<name>.txt` and see
   [Development](development.md).
9. **Docs.** Add a row to the coverage table in `services.md`, a page if the
   service has behaviour worth documenting, and a nav entry in `zensical.toml`.

## Things that bite

- The registered name must match the SigV4 signing name, which is not always
  the model's `endpointPrefix`.
- A service constructed before the services it depends on will not compile;
  follow the order in `main.rs`.
- Background workers (S3 notifications, Lambda, EventBridge) start after the
  listener binds, not in the constructor.
- A new service is not reachable from CloudFormation unless the stack engine
  knows its resource types.
