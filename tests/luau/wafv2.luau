--!strict
local function call(op, input) return test.call("wafv2", op, input) end
local function fails(op, input, code)
    test.expect_error(code, function() return test.try_call("wafv2", op, input) end)
end
local function extend(source: any, values: any): any
    local result = table.clone(source)
    for key, value in values do result[key] = value end
    return result
end
local invalid, nonexistent, lock = "WAFInvalidParameterException", "WAFNonexistentItemException", "WAFOptimisticLockException"
local vis = {SampledRequestsEnabled = true, CloudWatchMetricsEnabled = true, MetricName = "demo"}
local function rule(name, priority, statement, action)
    return {Name = name, Priority = priority, Statement = statement, VisibilityConfig = vis, Action = action}
end
local match = {ByteMatchStatement = {SearchString = "YWRtaW4=", FieldToMatch = {UriPath = {}},
    TextTransformations = {{Priority = 0, Type = "NONE"}}, PositionalConstraint = "CONTAINS"}}

test.suite("WAF IP sets", function(suite)
    local id: any = {}
    local arn = ""
    suite.before_all(function()
        local created = call("CreateIPSet", {Name = "office", Scope = "REGIONAL", IPAddressVersion = "IPV4",
            Addresses = {"192.0.2.0/24"}, Description = "office", Tags = {{Key = "env", Value = "test"}}}).Summary
        id = {Name = "office", Scope = "REGIONAL", Id = created.Id}; arn = created.ARN
    end)
    suite.case("validates IP ranges, scopes and duplicate names", function()
        fails("CreateIPSet", {Name = "office", Scope = "REGIONAL", IPAddressVersion = "IPV4", Addresses = roto.array({})}, "WAFDuplicateItemException")
        fails("CreateIPSet", {Name = "bad", Scope = "REGIONAL", IPAddressVersion = "IPV4", Addresses = {"192.0.2.0"}}, invalid)
        fails("CreateIPSet", {Name = "bad", Scope = "REGIONAL", IPAddressVersion = "IPV4", Addresses = {"2001:db8::/32"}}, invalid)
        fails("CreateIPSet", {Name = "bad", Scope = "GLOBAL", IPAddressVersion = "IPV4", Addresses = roto.array({})}, invalid)
        roto.set_region("eu-west-1")
        fails("CreateIPSet", {Name = "edge", Scope = "CLOUDFRONT", IPAddressVersion = "IPV4", Addresses = roto.array({})}, invalid)
        roto.set_region("us-east-1")
        fails("GetIPSet", extend(id, {Id = "00000000-0000-0000-0000-000000000000"}), nonexistent)
        local got = call("GetIPSet", id).IPSet
        test.equal(got.Addresses, {"192.0.2.0/24"}); test.equal(got.Description, "office")
        assert(string.find(arn, "arn:aws:wafv2:us%-east%-1:123456789012:regional/ipset/office/") == 1)
    end)
    suite.case("updates with optimistic locks and replaces omitted fields", function()
        local token = call("GetIPSet", id).LockToken
        fails("UpdateIPSet", extend(id, {Addresses = {"198.51.100.0/24"}, LockToken = "stale"}), lock)
        local nextToken = call("UpdateIPSet", extend(id, {Addresses = {"198.51.100.0/24"}, LockToken = token})).NextLockToken
        assert(nextToken ~= token)
        local again = call("GetIPSet", id)
        test.equal(again.LockToken, nextToken); test.equal(again.IPSet.Addresses, {"198.51.100.0/24"})
        assert(again.IPSet.Description == nil)
        fails("UpdateIPSet", extend(id, {Addresses = {"198.51.100.0/24"}, LockToken = token}), lock)
        call("UpdateIPSet", extend(id, {Addresses = roto.array({}), LockToken = nextToken}))
    end)
    suite.case("sorts and paginates IP sets", function()
        call("CreateIPSet", {Name = "six", Scope = "REGIONAL", IPAddressVersion = "IPV6", Addresses = {"2001:db8::/32"}})
        local first = call("ListIPSets", {Scope = "REGIONAL", Limit = 1})
        test.equal(first.IPSets[1].Name, "office"); assert(first.NextMarker)
        local second = call("ListIPSets", {Scope = "REGIONAL", Limit = 1, NextMarker = first.NextMarker})
        test.equal(second.IPSets[1].Name, "six"); assert(second.NextMarker == nil)
        fails("ListIPSets", {Scope = "REGIONAL", Limit = 0}, invalid)
        fails("ListIPSets", {Scope = "REGIONAL", NextMarker = "bad"}, invalid)
    end)
end)

test.suite("WAF rule groups and web ACLs", function(suite)
    local ipid: any, rgid: any, aclid: any = {}, {}, {}
    local iparn, rgarn, aclarn = "", "", ""
    local refs = {}
    suite.before_all(function()
        local ip = call("CreateIPSet", {Name = "office", Scope = "REGIONAL", IPAddressVersion = "IPV4", Addresses = {"192.0.2.0/24"}}).Summary
        ipid = {Name = "office", Scope = "REGIONAL", Id = ip.Id}; iparn = ip.ARN
        local rg = call("CreateRuleGroup", {Name = "rg", Scope = "REGIONAL", Capacity = 10, VisibilityConfig = vis,
            Rules = {rule("admin", 0, match, {Block = {}})}}).Summary
        rgid = {Name = "rg", Scope = "REGIONAL", Id = rg.Id}; rgarn = rg.ARN
        refs = {rule("block-office", 0, {IPSetReferenceStatement = {ARN = iparn}}, {Block = {}}),
            {Name = "shared", Priority = 1, Statement = {RuleGroupReferenceStatement = {ARN = rgarn}}, VisibilityConfig = vis, OverrideAction = {None = {}}}}
        local acl = call("CreateWebACL", {Name = "main", Scope = "REGIONAL", DefaultAction = {Allow = {}}, VisibilityConfig = vis,
            Rules = refs, Tags = {{Key = "tier", Value = "edge"}}}).Summary
        aclid = {Name = "main", Scope = "REGIONAL", Id = acl.Id}; aclarn = acl.ARN
    end)
    suite.case("validates rules and protects referenced resources", function()
        fails("CreateRuleGroup", {Name = "rg2", Scope = "REGIONAL", Capacity = 0, VisibilityConfig = vis}, invalid)
        fails("CreateRuleGroup", {Name = "rg2", Scope = "REGIONAL", Capacity = 10, VisibilityConfig = vis,
            Rules = {{Name = "admin", Priority = 0, Statement = match, VisibilityConfig = vis}}}, invalid)
        local rg = call("GetRuleGroup", rgid).RuleGroup
        test.equal(rg.Capacity, 10); test.equal(rg.LabelNamespace, "awswaf:123456789012:rulegroup:rg:")
        test.equal(call("GetRuleGroup", {ARN = rgarn}).RuleGroup.Id, rgid.Id)
        test.equal(call("GetWebACL", {ARN = aclarn}).WebACL.Id, aclid.Id)
        fails("DeleteIPSet", extend(ipid, {LockToken = call("GetIPSet", ipid).LockToken}), "WAFAssociatedItemException")
        fails("DeleteRuleGroup", extend(rgid, {LockToken = call("GetRuleGroup", rgid).LockToken}), "WAFAssociatedItemException")
        local bad = {Name = "bad", Scope = "REGIONAL", DefaultAction = {}, VisibilityConfig = vis, Rules = refs}
        fails("CreateWebACL", bad, invalid)
        fails("CreateWebACL", extend(bad, {DefaultAction = {Allow = {}, Block = {}}}), invalid)
        fails("CreateWebACL", extend(bad, {DefaultAction = {Allow = {}}, Rules = {refs[1], extend(refs[2], {Priority = 0})}}), invalid)
        fails("CreateWebACL", extend(bad, {DefaultAction = {Allow = {}}, Rules = {refs[1], extend(refs[2], {Name = "block-office"})}}), invalid)
        fails("CreateWebACL", extend(bad, {DefaultAction = {Allow = {}}, Rules = {rule("r", 0, {IPSetReferenceStatement = {ARN = string.gsub(iparn, "office", "gone")}}, {Allow = {}})}}), nonexistent)
        fails("CreateWebACL", extend(bad, {DefaultAction = {Allow = {}}, Rules = {rule("r", 0, {IPSetReferenceStatement = {ARN = string.gsub(iparn, "123456789012", "999999999999")}}, {Allow = {}})}}), invalid)
    end)
    suite.case("isolates CloudFront scope and supports deletion", function()
        local edge = call("CreateWebACL", {Name = "main", Scope = "CLOUDFRONT", DefaultAction = {Allow = {}}, VisibilityConfig = vis}).Summary
        assert(string.find(edge.ARN, ":global/webacl/main/", 1, true) ~= nil)
        test.equal(call("ListWebACLs", {Scope = "CLOUDFRONT"}).WebACLs[1].Id, edge.Id)
        local identity = {Name = "main", Scope = "CLOUDFRONT", Id = edge.Id}
        call("DeleteWebACL", extend(identity, {LockToken = call("GetWebACL", identity).LockToken}))
        fails("GetWebACL", identity, nonexistent)
    end)
    suite.case("associates and disassociates supported resources", function()
        local alb = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/web/50dc6c495c0c9188"
        local api = "arn:aws:apigateway:us-east-1::/restapis/abc123/stages/prod"
        fails("AssociateWebACL", {WebACLArn = aclarn, ResourceArn = "arn:aws:s3:::bucket"}, invalid)
        fails("AssociateWebACL", {WebACLArn = aclarn, ResourceArn = string.gsub(alb, "us%-east%-1", "eu-west-1")}, invalid)
        fails("AssociateWebACL", {WebACLArn = string.gsub(aclarn, "main", "gone"), ResourceArn = alb}, nonexistent)
        call("AssociateWebACL", {WebACLArn = aclarn, ResourceArn = alb}); call("AssociateWebACL", {WebACLArn = aclarn, ResourceArn = api})
        test.equal(call("GetWebACLForResource", {ResourceArn = alb}).WebACL.Name, "main")
        test.equal(call("ListResourcesForWebACL", {WebACLArn = aclarn}).ResourceArns, {api, alb})
        test.equal(call("ListResourcesForWebACL", {WebACLArn = aclarn, ResourceType = "API_GATEWAY"}).ResourceArns, {api})
        fails("ListResourcesForWebACL", {WebACLArn = aclarn, ResourceType = "BOGUS"}, invalid)
        fails("DeleteWebACL", extend(aclid, {LockToken = call("GetWebACL", aclid).LockToken}), "WAFAssociatedItemException")
        call("DisassociateWebACL", {ResourceArn = api}); call("DisassociateWebACL", {ResourceArn = api})
        test.equal(call("GetWebACLForResource", {ResourceArn = api}), {})
        call("DisassociateWebACL", {ResourceArn = alb})
    end)
    suite.case("updates an ACL using its lock token", function()
        local token = call("GetWebACL", aclid).LockToken
        local update = extend(aclid, {DefaultAction = {Block = {}}, VisibilityConfig = vis, Rules = refs, LockToken = token})
        fails("UpdateWebACL", extend(update, {LockToken = "stale"}), lock)
        fails("UpdateWebACL", extend(update, {DefaultAction = {}}), invalid)
        call("UpdateWebACL", update)
        test.equal(call("GetWebACL", aclid).WebACL.DefaultAction, {Block = {}})
    end)
    suite.case("tags ACLs and validates resource ARNs", function()
        call("TagResource", {ResourceARN = aclarn, Tags = {{Key = "tier", Value = "core"}, {Key = "drop", Value = "x"}}})
        call("UntagResource", {ResourceARN = aclarn, TagKeys = {"drop"}})
        test.equal(call("ListTagsForResource", {ResourceARN = aclarn}).TagInfoForResource.TagList, {{Key = "tier", Value = "core"}})
        fails("ListTagsForResource", {ResourceARN = string.gsub(aclarn, "us%-east%-1", "eu-west-1")}, invalid)
        fails("ListTagsForResource", {ResourceARN = string.gsub(aclarn, "main", "gone")}, nonexistent)
        fails("TagResource", {ResourceARN = "arn:aws:s3:::bucket", Tags = {{Key = "a", Value = "b"}}}, invalid)
    end)
end)

test.case("WAF rule group updates reject stale locks", function()
    local created = call("CreateRuleGroup", {Name = "tmp", Scope = "REGIONAL", Capacity = 5, VisibilityConfig = vis, Description = "temporary"}).Summary
    local id = {Name = "tmp", Scope = "REGIONAL", Id = created.Id}
    local token = call("GetRuleGroup", id).LockToken
    local update = extend(id, {VisibilityConfig = vis, Rules = {rule("admin", 0, match, {Count = {}})}, LockToken = token})
    fails("UpdateRuleGroup", extend(update, {LockToken = "stale"}), lock)
    call("UpdateRuleGroup", update)
    local changed = call("GetRuleGroup", id)
    test.equal(changed.RuleGroup.Capacity, 5); assert(changed.RuleGroup.Description == nil); test.equal(#changed.RuleGroup.Rules, 1)
    fails("DeleteRuleGroup", extend(id, {LockToken = token}), lock)
    call("DeleteRuleGroup", extend(id, {LockToken = changed.LockToken}))
    fails("GetRuleGroup", id, nonexistent)
end)
