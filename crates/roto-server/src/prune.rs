// Reduces a botocore model to chosen operations and the shapes they reach. Shared by
// `build.rs` (which embeds only the typed slice of the models used for types alone) and the
// Luau definitions generator, so it uses fully qualified paths and no `use` items.

fn reach(shapes: &serde_json::Value, name: &str, seen: &mut std::collections::BTreeSet<String>) {
    if !seen.insert(name.to_owned()) {
        return;
    }
    let shape = &shapes[name];
    let mut follow = |spec: &serde_json::Value| {
        if let Some(inner) = spec["shape"].as_str() {
            reach(shapes, inner, seen);
        }
    };
    if let Some(members) = shape["members"].as_object() {
        members.values().for_each(&mut follow);
    }
    for key in ["member", "key", "value"] {
        follow(&shape[key]);
    }
}

/// Keeps only `operations` and the shapes reachable from their input and output.
#[allow(dead_code)] // each includer uses it; the test build of one may not
pub fn prune(model: &mut serde_json::Value, operations: &[String]) {
    if let Some(ops) = model["operations"].as_object_mut() {
        ops.retain(|name, _| operations.contains(name));
    }
    let mut seen = std::collections::BTreeSet::new();
    let roots: Vec<String> = model["operations"]
        .as_object()
        .into_iter()
        .flat_map(|ops| ops.values())
        .flat_map(|op| {
            ["input", "output"].map(|side| op[side]["shape"].as_str().map(str::to_owned))
        })
        .flatten()
        .collect();
    for root in roots {
        reach(&model["shapes"], &root, &mut seen);
    }
    if let Some(shapes) = model["shapes"].as_object_mut() {
        shapes.retain(|name, _| seen.contains(name));
    }
}
