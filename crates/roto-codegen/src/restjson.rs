//! REST-JSON HTTP bindings reuse the generated JSON shapes and service trait.
use std::fmt::Write as _;

use super::Generator;

impl Generator<'_> {
    pub(super) fn gen_rest_json(&self, out: &mut String) {
        out.push_str("pub const ROUTES: &[roto_protocol::restjson::Route] = &[\n");
        for (name, op) in self.model["operations"].as_object().unwrap() {
            let _ = writeln!(
                out,
                "roto_protocol::restjson::Route {{ operation: {name:?}, method: {:?}, path: {:?}, status: {}, input: &[",
                op["http"]["method"].as_str().unwrap(),
                op["http"]["requestUri"].as_str().unwrap(),
                op["http"]["responseCode"].as_u64().unwrap_or(200)
            );
            for side in ["input", "output"] {
                if side == "output" {
                    out.push_str("], output: &[\n");
                }
                if let Some(shape) = op[side]["shape"].as_str() {
                    let s = self.shape(shape);
                    for (member, m) in s["members"].as_object().into_iter().flatten() {
                        let payload = s["payload"].as_str() == Some(member);
                        let location = if payload {
                            "payload"
                        } else {
                            m["location"].as_str().unwrap_or("body")
                        };
                        if location == "body" {
                            continue;
                        }
                        let wire = m["locationName"].as_str().unwrap_or(member);
                        let kind = self.shape(m["shape"].as_str().unwrap())["type"]
                            .as_str()
                            .unwrap_or("string");
                        let _ = writeln!(
                            out,
                            "roto_protocol::restjson::Binding {{ member: {member:?}, wire: {wire:?}, location: {location:?}, kind: {kind:?} }},"
                        );
                    }
                }
            }
            out.push_str("] },\n");
        }
        out.push_str("];\npub fn dispatch_http<S: Service + ?Sized>(svc: &S, ctx: &RequestContext, req: &roto_core::RawRequest) -> Result<RawResponse, AwsError> {\nlet (route, body) = roto_protocol::restjson::decode(ROUTES, req)?;\nlet response = dispatch(svc, ctx, route.operation, &body)?;\nroto_protocol::restjson::encode(route, response)\n}\n");
    }
}
