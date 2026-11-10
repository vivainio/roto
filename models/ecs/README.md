`response-model.json` is derived from the ECS 2014-11-13 botocore service model
(botocore 1.43.126). It contains operation output references and their reachable
shapes, with documentation removed. Structure member and required-field names
are capitalized to match the Step Functions ECS integration's response format;
map keys and values retain their original model shapes. It is used only for
`TestState` mock validation, not for the ECS wire protocol.
