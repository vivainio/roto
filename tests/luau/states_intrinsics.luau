--!strict
local function evaluate(parameters, input)
    return test.aws.states.TestState({definition = roto.json_encode({Type = "Pass", Parameters = parameters, End = true}),
        input = roto.json_encode(input)})
end

test.case("array intrinsics preserve structure and compare objects deeply", function()
    local result = evaluate({
        ["unique.$"] = "States.ArrayUnique($.items)",
        ["contains.$"] = "States.ArrayContains($.items, $.find)",
        ["chunks.$"] = "States.ArrayPartition($.numbers, 2)",
        ["range.$"] = "States.ArrayRange(1, 5, 2)",
    }, {items = {{x = 1, y = 2}, {y = 2, x = 1}, {x = 3}}, find = {y = 2, x = 1}, numbers = {1, 2, 3, 4, 5}})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(result.output)), {unique = {{x = 1, y = 2}, {x = 3}},
        contains = true, chunks = {{1, 2}, {3, 4}, {5}}, range = {1, 3, 5}})
end)

test.case("string intrinsics handle JSON, base64 and Unicode separators", function()
    local result = evaluate({
        ["encoded.$"] = "States.Base64Encode($.text)",
        ["decoded.$"] = "States.Base64Decode($.encoded)",
        ["parsed.$"] = "States.StringToJson($.json)",
        ["split.$"] = "States.StringSplit($.words, '☃')",
    }, {text = "hello", encoded = "aGVsbG8=", json = '{"a":[1,true]}', words = "one☃two☃three"})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(result.output)), {encoded = "aGVsbG8=", decoded = "hello",
        parsed = {a = {1, true}}, split = {"one", "two", "three"}})
end)

test.case("math random with a seed is stable across calls", function()
    local result = evaluate({["one.$"] = "States.MathRandom(1, 100, 42)", ["two.$"] = "States.MathRandom(1, 100, 42)",
        ["sum.$"] = "States.MathAdd(3, 4)"}, {})
    test.equal(result.status, "SUCCEEDED")
    local output = roto.json_decode(assert(result.output))
    test.equal(output.one, output.two)
    assert(output.one >= 1 and output.one < 100)
    test.equal(output.sum, 7)
end)

test.case("invalid intrinsic arguments surface States.Runtime", function()
    for _, expression in {
        "States.ArrayGetItem($.items, 5)",
        "States.ArrayPartition($.items, 0)",
        "States.MathRandom(3, 3)",
        "States.Format('{} {}', 'one')",
        "States.Base64Decode('not base64!')",
    } do
        local result = evaluate({["value.$"] = expression}, {items = {1, 2}})
        test.equal(result.status, "FAILED")
        test.equal(result.error, "States.Runtime")
    end
end)

test.case("nested paths and execution context feed templates", function()
    local result = evaluate({
        ["name.$"] = "$$.Execution.Name",
        ["selected.$"] = "$.records[1].id",
        ["all.$"] = "$.records[*].id",
        ["greeting.$"] = "States.Format('hello {}', $.records[0].id)",
    }, {records = {{id = "a"}, {id = "b"}}})
    test.equal(result.status, "SUCCEEDED")
    test.equal(roto.json_decode(assert(result.output)), {name = "TestState", selected = "b", all = {"a", "b"}, greeting = "hello a"})
end)
