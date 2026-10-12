//! In-memory API Gateway v2 WebSocket APIs and their control-plane subset.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use roto_core::{RawRequest, RawResponse};
use serde_json::{Value, json};

#[derive(Clone, Default)]
pub struct Registry(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    apis: HashMap<String, Api>,
}

#[derive(Clone)]
struct Api {
    api_id: String,
    api_endpoint: String,
    name: String,
    protocol_type: String,
    route_selection_expression: String,
    integrations: HashMap<String, Integration>,
    routes: HashMap<String, Route>,
    route_responses: HashMap<String, RouteResponse>,
    stages: HashMap<String, Stage>,
    deployments: HashMap<String, Deployment>,
}

#[derive(Clone)]
struct Integration {
    integration_id: String,
    integration_type: String,
    integration_uri: String,
    integration_method: String,
    payload_format_version: Option<String>,
    timeout_in_millis: u64,
}

#[derive(Clone)]
struct Route {
    route_id: String,
    route_key: String,
    target: String,
}

#[derive(Clone)]
struct RouteResponse {
    route_response_id: String,
    route_response_key: String,
    route_id: String,
}

#[derive(Clone)]
struct Stage {
    stage_name: String,
    auto_deploy: bool,
    deployment_id: Option<String>,
}

#[derive(Clone)]
struct Deployment {
    deployment_id: String,
    routes: HashMap<String, String>,
}

#[derive(Clone)]
pub struct RuntimeApi {
    pub api_id: String,
    pub stage_name: String,
    pub route_selection_expression: String,
    pub integrations: HashMap<String, String>,
    pub routes: HashMap<String, String>,
    pub route_responses: HashSet<String>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn new_id() -> String {
    format!("{:010x}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

impl Registry {
    pub fn reset(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.apis.clear();
        }
    }

    pub fn handle(&self, request: &RawRequest) -> RawResponse {
        let body: Value = if request.body.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice::<Value>(&request.body) {
                Ok(value) if value.is_object() => value,
                _ => return error(400, "Request body must be a JSON object"),
            }
        };
        let path: Vec<String> = request
            .path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(roto_protocol::restxml::percent_decode_path)
            .collect();
        if path.as_slice() == ["v2", "apis"] {
            return match request.method.as_str() {
                "GET" => self.list_apis(),
                "POST" => self.create_api(&body, request),
                _ => method_not_allowed(),
            };
        }
        if path.len() < 3 || path[0] != "v2" || path[1] != "apis" {
            return error(404, "Not found");
        }
        let api_id = &path[2];
        if path.len() == 3 {
            return self.api_resource(api_id, &request.method);
        }
        if path.len() == 4 {
            return self.api_collection(api_id, &path[3], &request.method, &body);
        }
        if path.len() == 5 {
            return self.api_item(api_id, &path[3], &path[4], &request.method);
        }
        if path.len() == 6 {
            return self.api_nested(api_id, &path[3], &path[4], &path[5], &request.method, &body);
        }
        if path.len() == 7 {
            return self.api_nested_item(
                api_id,
                &path[3],
                &path[4],
                &path[5],
                &path[6],
                &request.method,
            );
        }
        error(404, "Not found")
    }

    fn list_apis(&self) -> RawResponse {
        let Ok(state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        ok(json!({"items": state.apis.values().map(api_json).collect::<Vec<_>>() }))
    }

    fn create_api(&self, body: &Value, request: &RawRequest) -> RawResponse {
        let Some(name) = string(body, "Name") else {
            return error(400, "Name is required");
        };
        if string(body, "ProtocolType") != Some("WEBSOCKET") {
            return error(400, "Only ProtocolType WEBSOCKET is supported");
        }
        let Some(route_selection_expression) = string(body, "RouteSelectionExpression") else {
            return error(
                400,
                "RouteSelectionExpression is required for WebSocket APIs",
            );
        };
        let api = Api {
            api_id: new_id(),
            api_endpoint: format!("http://{}", request.header("host").unwrap_or("localhost")),
            name: name.into(),
            protocol_type: "WEBSOCKET".into(),
            route_selection_expression: route_selection_expression.into(),
            integrations: HashMap::new(),
            routes: HashMap::new(),
            route_responses: HashMap::new(),
            stages: HashMap::new(),
            deployments: HashMap::new(),
        };
        let output = api_json(&api);
        let Ok(mut state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        state.apis.insert(api.api_id.clone(), api);
        created(output)
    }

    fn api_resource(&self, api_id: &str, method: &str) -> RawResponse {
        let Ok(mut state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        let Some(api) = state.apis.get(api_id) else {
            return error(404, "Not found");
        };
        match method {
            "GET" => ok(api_json(api)),
            "DELETE" => {
                state.apis.remove(api_id);
                empty(204)
            }
            _ => method_not_allowed(),
        }
    }

    fn api_collection(&self, api_id: &str, kind: &str, method: &str, body: &Value) -> RawResponse {
        let Ok(mut state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        let Some(api) = state.apis.get_mut(api_id) else {
            return error(404, "Not found");
        };
        match (kind, method) {
            ("integrations", "GET") => ok(
                json!({"items": api.integrations.values().map(integration_json).collect::<Vec<_>>() }),
            ),
            ("integrations", "POST") => {
                let integration_type = string(body, "IntegrationType").unwrap_or("AWS_PROXY");
                if integration_type != "AWS_PROXY" {
                    return error(400, "Only AWS_PROXY integrations are supported");
                }
                let Some(uri) = string(body, "IntegrationUri") else {
                    return error(400, "IntegrationUri is required");
                };
                let integration = Integration {
                    integration_id: new_id(),
                    integration_type: integration_type.into(),
                    integration_uri: uri.into(),
                    integration_method: string(body, "IntegrationMethod").unwrap_or("POST").into(),
                    payload_format_version: string(body, "PayloadFormatVersion").map(str::to_owned),
                    timeout_in_millis: body
                        .get("TimeoutInMillis")
                        .and_then(Value::as_u64)
                        .unwrap_or(29000),
                };
                let output = integration_json(&integration);
                api.integrations
                    .insert(integration.integration_id.clone(), integration);
                created(output)
            }
            ("routes", "GET") => {
                ok(json!({"items": api.routes.values().map(route_json).collect::<Vec<_>>() }))
            }
            ("routes", "POST") => {
                let Some(route_key) = string(body, "RouteKey") else {
                    return error(400, "RouteKey is required");
                };
                if api.routes.contains_key(route_key) {
                    return error(409, "A route with this RouteKey already exists");
                }
                let Some(target) = string(body, "Target") else {
                    return error(400, "Target is required");
                };
                if integration_id(target).is_none_or(|id| !api.integrations.contains_key(id)) {
                    return error(400, "Target references an unknown integration");
                }
                let route = Route {
                    route_id: new_id(),
                    route_key: route_key.into(),
                    target: target.into(),
                };
                let output = route_json(&route);
                api.routes.insert(route.route_key.clone(), route);
                created(output)
            }
            ("stages", "GET") => {
                ok(json!({"items": api.stages.values().map(stage_json).collect::<Vec<_>>() }))
            }
            ("stages", "POST") => {
                let Some(stage_name) = string(body, "StageName") else {
                    return error(400, "StageName is required");
                };
                if api.stages.contains_key(stage_name) {
                    return error(409, "Stage already exists");
                }
                let auto_deploy = body
                    .get("AutoDeploy")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let deployment_id = string(body, "DeploymentId").map(str::to_owned);
                if deployment_id
                    .as_ref()
                    .is_some_and(|id| !api.deployments.contains_key(id))
                {
                    return error(400, "DeploymentId references an unknown deployment");
                }
                let stage = Stage {
                    stage_name: stage_name.into(),
                    auto_deploy,
                    deployment_id,
                };
                let output = stage_json(&stage);
                api.stages.insert(stage.stage_name.clone(), stage);
                created(output)
            }
            ("deployments", "POST") => {
                let deployment_id = new_id();
                let routes = runtime_routes(api);
                let deployment = Deployment {
                    deployment_id: deployment_id.clone(),
                    routes,
                };
                let mut output = json!({"DeploymentId": deployment_id});
                if let Some(description) = string(body, "Description") {
                    output["Description"] = json!(description);
                }
                api.deployments.insert(deployment_id.clone(), deployment);
                if let Some(stage_name) = string(body, "StageName") {
                    let stage = api.stages.entry(stage_name.into()).or_insert(Stage {
                        stage_name: stage_name.into(),
                        auto_deploy: false,
                        deployment_id: None,
                    });
                    stage.deployment_id = Some(deployment_id);
                }
                created(output)
            }
            _ => method_not_allowed(),
        }
    }

    fn api_item(&self, api_id: &str, kind: &str, item_id: &str, method: &str) -> RawResponse {
        let Ok(mut state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        let Some(api) = state.apis.get_mut(api_id) else {
            return error(404, "Not found");
        };
        match (kind, method) {
            ("integrations", "GET") => api
                .integrations
                .get(item_id)
                .map(integration_json)
                .map(ok)
                .unwrap_or_else(|| error(404, "Not found")),
            ("integrations", "DELETE") => api
                .integrations
                .remove(item_id)
                .map(|_| empty(204))
                .unwrap_or_else(|| error(404, "Not found")),
            ("routes", "GET") => api
                .routes
                .values()
                .find(|route| route.route_id == item_id)
                .map(route_json)
                .map(ok)
                .unwrap_or_else(|| error(404, "Not found")),
            ("routes", "DELETE") => {
                let Some(key) = api
                    .routes
                    .iter()
                    .find_map(|(key, route)| (route.route_id == item_id).then(|| key.clone()))
                else {
                    return error(404, "Not found");
                };
                api.routes.remove(&key);
                empty(204)
            }
            ("stages", "GET") => api
                .stages
                .get(item_id)
                .map(stage_json)
                .map(ok)
                .unwrap_or_else(|| error(404, "Not found")),
            ("stages", "DELETE") => api
                .stages
                .remove(item_id)
                .map(|_| empty(204))
                .unwrap_or_else(|| error(404, "Not found")),
            ("deployments", "GET") => api
                .deployments
                .get(item_id)
                .map(|deployment| json!({"DeploymentId":deployment.deployment_id}))
                .map(ok)
                .unwrap_or_else(|| error(404, "Not found")),
            ("deployments", "DELETE") => api
                .deployments
                .remove(item_id)
                .map(|_| empty(204))
                .unwrap_or_else(|| error(404, "Not found")),
            _ => method_not_allowed(),
        }
    }

    fn api_nested(
        &self,
        api_id: &str,
        kind: &str,
        route_id: &str,
        nested: &str,
        method: &str,
        body: &Value,
    ) -> RawResponse {
        if kind != "routes" || nested != "routeresponses" {
            return error(404, "Not found");
        }
        let Ok(mut state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        let Some(api) = state.apis.get_mut(api_id) else {
            return error(404, "Not found");
        };
        if !api.routes.values().any(|route| route.route_id == route_id) {
            return error(404, "Not found");
        }
        match method {
            "GET" => ok(
                json!({"items":api.route_responses.values().filter(|item| item.route_id == route_id).map(route_response_json).collect::<Vec<_>>() }),
            ),
            "POST" => {
                let Some(route_response_key) = string(body, "RouteResponseKey") else {
                    return error(400, "RouteResponseKey is required");
                };
                if route_response_key != "$default" {
                    return error(400, "Only the $default route response is supported");
                }
                if api.route_responses.values().any(|item| {
                    item.route_id == route_id && item.route_response_key == route_response_key
                }) {
                    return error(409, "Route response already exists");
                }
                let response = RouteResponse {
                    route_response_id: new_id(),
                    route_response_key: route_response_key.into(),
                    route_id: route_id.into(),
                };
                let output = route_response_json(&response);
                api.route_responses
                    .insert(format!("{route_id}:{route_response_key}"), response);
                created(output)
            }
            _ => method_not_allowed(),
        }
    }

    fn api_nested_item(
        &self,
        api_id: &str,
        kind: &str,
        route_id: &str,
        nested: &str,
        item_id: &str,
        method: &str,
    ) -> RawResponse {
        if kind != "routes" || nested != "routeresponses" {
            return error(404, "Not found");
        }
        let Ok(mut state) = self.0.lock() else {
            return error(500, "Internal server error");
        };
        let Some(api) = state.apis.get_mut(api_id) else {
            return error(404, "Not found");
        };
        match method {
            "GET" => api
                .route_responses
                .values()
                .find(|item| item.route_id == route_id && item.route_response_id == item_id)
                .map(route_response_json)
                .map(ok)
                .unwrap_or_else(|| error(404, "Not found")),
            "DELETE" => {
                let key = api.route_responses.iter().find_map(|(key, item)| {
                    (item.route_id == route_id && item.route_response_id == item_id)
                        .then(|| key.clone())
                });
                key.and_then(|key| api.route_responses.remove(&key))
                    .map(|_| empty(204))
                    .unwrap_or_else(|| error(404, "Not found"))
            }
            _ => method_not_allowed(),
        }
    }

    pub fn runtime_api(&self, api_id: &str, stage_name: &str) -> Option<RuntimeApi> {
        let state = self.0.lock().ok()?;
        let api = state.apis.get(api_id)?;
        if api.protocol_type != "WEBSOCKET" {
            return None;
        }
        let stage = api.stages.get(stage_name)?;
        let (routes, integrations) = if stage.auto_deploy {
            (
                runtime_routes(api),
                api.integrations
                    .iter()
                    .map(|(id, integration)| (id.clone(), integration.integration_uri.clone()))
                    .collect(),
            )
        } else {
            let deployment = api.deployments.get(stage.deployment_id.as_deref()?)?;
            let integrations = api
                .integrations
                .iter()
                .map(|(id, integration)| (id.clone(), integration.integration_uri.clone()))
                .collect();
            (deployment.routes.clone(), integrations)
        };
        Some(RuntimeApi {
            api_id: api.api_id.clone(),
            stage_name: stage.stage_name.clone(),
            route_selection_expression: api.route_selection_expression.clone(),
            integrations,
            routes,
            route_responses: api
                .route_responses
                .values()
                .filter_map(|response| {
                    api.routes
                        .values()
                        .find(|route| route.route_id == response.route_id)
                        .map(|route| route.route_key.clone())
                })
                .collect(),
        })
    }
}

fn runtime_routes(api: &Api) -> HashMap<String, String> {
    api.routes
        .values()
        .filter_map(|route| {
            integration_id(&route.target)
                .map(|integration| (route.route_key.clone(), integration.to_owned()))
        })
        .collect()
}

fn integration_id(target: &str) -> Option<&str> {
    target
        .strip_prefix("integrations/")
        .filter(|id| !id.is_empty())
}

fn api_json(api: &Api) -> Value {
    json!({
        "apiId": api.api_id,
        "apiEndpoint": api.api_endpoint,
        "name": api.name,
        "protocolType": api.protocol_type,
        "routeSelectionExpression": api.route_selection_expression,
    })
}

fn integration_json(integration: &Integration) -> Value {
    let mut value = json!({
        "integrationId": integration.integration_id,
        "integrationType": integration.integration_type,
        "integrationUri": integration.integration_uri,
        "integrationMethod": integration.integration_method,
        "timeoutInMillis": integration.timeout_in_millis,
    });
    if let Some(version) = &integration.payload_format_version {
        value["payloadFormatVersion"] = json!(version);
    }
    value
}

fn route_json(route: &Route) -> Value {
    json!({"routeId":route.route_id,"routeKey":route.route_key,"target":route.target})
}

fn route_response_json(response: &RouteResponse) -> Value {
    json!({"routeResponseId":response.route_response_id,"routeResponseKey":response.route_response_key})
}

fn stage_json(stage: &Stage) -> Value {
    let mut value = json!({"stageName":stage.stage_name,"autoDeploy":stage.auto_deploy});
    if let Some(deployment_id) = &stage.deployment_id {
        value["deploymentId"] = json!(deployment_id);
    }
    value
}

fn string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

fn ok(value: Value) -> RawResponse {
    response(200, value)
}

fn created(value: Value) -> RawResponse {
    response(201, value)
}

fn error(status: u16, message: &str) -> RawResponse {
    response(status, json!({"message":message}))
}

fn response(status: u16, value: Value) -> RawResponse {
    RawResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(&value).unwrap_or_default(),
    }
}

fn empty(status: u16) -> RawResponse {
    RawResponse {
        status,
        headers: vec![],
        body: vec![],
    }
}

pub fn operation(request: &RawRequest) -> Option<String> {
    let path: Vec<_> = request
        .path
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    match (request.method.as_str(), path.as_slice()) {
        ("GET", ["v2", "apis"]) => Some("GetApis".into()),
        ("POST", ["v2", "apis"]) => Some("CreateApi".into()),
        ("GET", ["v2", "apis", _]) => Some("GetApi".into()),
        ("DELETE", ["v2", "apis", _]) => Some("DeleteApi".into()),
        ("GET", ["v2", "apis", _, "routes"]) => Some("GetRoutes".into()),
        ("POST", ["v2", "apis", _, "routes"]) => Some("CreateRoute".into()),
        ("GET", ["v2", "apis", _, "integrations"]) => Some("GetIntegrations".into()),
        ("POST", ["v2", "apis", _, "integrations"]) => Some("CreateIntegration".into()),
        ("GET", ["v2", "apis", _, "stages"]) => Some("GetStages".into()),
        ("POST", ["v2", "apis", _, "stages"]) => Some("CreateStage".into()),
        ("POST", ["v2", "apis", _, "deployments"]) => Some("CreateDeployment".into()),
        ("GET", ["v2", "apis", _, "routes", _, "routeresponses"]) => {
            Some("GetRouteResponses".into())
        }
        ("POST", ["v2", "apis", _, "routes", _, "routeresponses"]) => {
            Some("CreateRouteResponse".into())
        }
        ("GET", ["v2", "apis", _, "routes", _]) => Some("GetRoute".into()),
        ("DELETE", ["v2", "apis", _, "routes", _]) => Some("DeleteRoute".into()),
        ("GET", ["v2", "apis", _, "integrations", _]) => Some("GetIntegration".into()),
        ("DELETE", ["v2", "apis", _, "integrations", _]) => Some("DeleteIntegration".into()),
        ("GET", ["v2", "apis", _, "stages", _]) => Some("GetStage".into()),
        ("DELETE", ["v2", "apis", _, "stages", _]) => Some("DeleteStage".into()),
        _ => None,
    }
}

fn method_not_allowed() -> RawResponse {
    error(405, "Method not allowed")
}
