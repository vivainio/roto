--!strict
-- Ported from the CUR, ACM, Budgets, and Config HTTP smoke checks.
local account = "123456789012"
local function call(service, operation, input)
    return test.call(service, operation, input)
end
local function errorCode(service, operation, input, code)
    test.expect_error(code, function() return test.try_call(service, operation, input) end)
end

test.case("CUR report lifecycle, tags, scope and pagination", function()
    local definition = {ReportName = "report", TimeUnit = "DAILY", Format = "textORcsv", Compression = "GZIP",
        AdditionalSchemaElements = {"RESOURCES"}, S3Bucket = "billing", S3Prefix = "reports",
        S3Region = "us-east-1", AdditionalArtifacts = roto.array({})}
    local function c(op, input) return call("cur", op, input) end
    test.equal(c("DescribeReportDefinitions", {}).ReportDefinitions, {})
    c("PutReportDefinition", {ReportDefinition = definition})
    errorCode("cur", "PutReportDefinition", {ReportDefinition = definition}, "DuplicateReportNameException")
    c("TagResource", {ReportName = "report", Tags = {{Key = "env", Value = "dev"}}})
    c("TagResource", {ReportName = "report", Tags = {{Key = "env", Value = "prod"}}})
    errorCode("cur", "TagResource", {ReportName = "report", Tags = {{Key = "new", Value = "yes"}, {Key = "bad", Value = 1}}}, "ValidationException")
    test.equal(c("ListTagsForResource", {ReportName = "report"}).Tags, {{Key = "env", Value = "prod"}})
    c("UntagResource", {ReportName = "report", TagKeys = {"env", "missing"}})
    test.equal(c("ListTagsForResource", {ReportName = "report"}).Tags, {})
    local modified = table.clone(definition)
    modified.ReportName = "changed"; modified.S3Prefix = "new"
    c("ModifyReportDefinition", {ReportName = "report", ReportDefinition = modified})
    roto.set_region("eu-west-1")
    test.equal(c("DescribeReportDefinitions", {}).ReportDefinitions[1].ReportName, "report")
    roto.set_region("us-east-1"); roto.set_account("999999999999")
    test.equal(c("DescribeReportDefinitions", {}).ReportDefinitions, {})
    roto.set_account(account)
    for i = 0, 5 do
        local report = table.clone(definition); report.ReportName = "report-" .. i
        c("PutReportDefinition", {ReportDefinition = report})
    end
    local page = c("DescribeReportDefinitions", {MaxResults = 5})
    test.equal(#page.ReportDefinitions, 5)
    test.equal(#c("DescribeReportDefinitions", {NextToken = page.NextToken}).ReportDefinitions, 2)
    errorCode("cur", "DescribeReportDefinitions", {NextToken = "bad"}, "ValidationException")
    c("DeleteReportDefinition", {ReportName = "report-0"})
    errorCode("cur", "DeleteReportDefinition", {ReportName = "missing"}, "ValidationException")
    errorCode("cur", "ListTagsForResource", {ReportName = "missing"}, "ResourceNotFoundException")
    errorCode("cur", "PutReportDefinition", {ReportDefinition = roto.array({})}, "ValidationException")
end)

test.case("ACM certificate lifecycle, scope, tags and pagination", function()
    local function c(op, input) return call("acm", op, input) end
    test.equal(c("ListCertificates", {}).CertificateSummaryList, {})
    local arn = c("RequestCertificate", {DomainName = "example.com", SubjectAlternativeNames = {"*.example.com"}}).CertificateArn
    local cert = c("DescribeCertificate", {CertificateArn = arn}).Certificate
    test.equal(cert.Status, "ISSUED")
    test.equal(cert.DomainValidationOptions[1].ResourceRecord, cert.DomainValidationOptions[2].ResourceRecord)
    c("AddTagsToCertificate", {CertificateArn = arn, Tags = {{Key = "env", Value = "dev"}}})
    c("RemoveTagsFromCertificate", {CertificateArn = arn, Tags = {{Key = "env"}}})
    test.equal(c("ListTagsForCertificate", {CertificateArn = arn}).Tags, {})
    c("UpdateCertificateOptions", {CertificateArn = arn, Options = {CertificateTransparencyLoggingPreference = "DISABLED"}})
    test.equal(c("DescribeCertificate", {CertificateArn = arn}).Certificate.Options.CertificateTransparencyLoggingPreference, "DISABLED")
    roto.set_region("eu-west-1")
    errorCode("acm", "DescribeCertificate", {CertificateArn = arn}, "ResourceNotFoundException")
    roto.set_region("us-east-1"); roto.set_account("999999999999")
    errorCode("acm", "DescribeCertificate", {CertificateArn = arn}, "ResourceNotFoundException")
    roto.set_account(account)
    c("RequestCertificate", {DomainName = "other.example.com"})
    local page = c("ListCertificates", {MaxItems = 1})
    test.equal(#page.CertificateSummaryList, 1); assert(page.NextToken)
    assert(c("ListCertificates", {MaxItems = 1, NextToken = page.NextToken}).NextToken == nil)
    local pem = "-----BEGIN CERTIFICATE-----\nhello\n-----END CERTIFICATE-----\n"
    local payload = {Certificate = roto.encode_base64(pem), CertificateChain = roto.encode_base64("chain-data"),
        PrivateKey = roto.encode_base64("private-do-not-store")}
    local imported = c("ImportCertificate", payload).CertificateArn
    test.equal(c("GetCertificate", {CertificateArn = imported}), {Certificate = pem, CertificateChain = "chain-data"})
    assert(string.find(c("DescribeCertificate", {CertificateArn = imported}).Certificate.DomainName, "%.invalid$") ~= nil)
    local replacement = {CertificateArn = imported, Certificate = roto.encode_base64("replacement"),
        CertificateChain = payload.CertificateChain, PrivateKey = payload.PrivateKey}
    test.equal(c("ImportCertificate", replacement).CertificateArn, imported)
    test.equal(c("GetCertificate", {CertificateArn = imported}).Certificate, "replacement")
    replacement.CertificateArn = string.gsub(arn, account, "999999999999")
    errorCode("acm", "ImportCertificate", replacement, "ResourceNotFoundException")
    c("DeleteCertificate", {CertificateArn = imported})
    errorCode("acm", "GetCertificate", {CertificateArn = imported}, "ResourceNotFoundException")
    errorCode("acm", "RenewCertificate", {CertificateArn = arn}, "NotImplemented")
end)

test.case("Config rules, recorder and delivery channel", function()
    local function c(op, input) return call("config", op, input) end
    local rule = {ConfigRuleName = "versioning", Source = {Owner = "AWS", SourceIdentifier = "S3_BUCKET_VERSIONING_ENABLED"}}
    c("PutConfigRule", {ConfigRule = rule})
    local original = c("DescribeConfigRules", {}).ConfigRules[1]
    local updated = table.clone(rule); updated.Description = "updated"
    c("PutConfigRule", {ConfigRule = updated})
    test.equal(c("DescribeConfigRules", {ConfigRuleNames = {"versioning"}}).ConfigRules[1].ConfigRuleId, original.ConfigRuleId)
    errorCode("config", "DescribeConfigRules", {ConfigRuleNames = {"missing"}}, "NoSuchConfigRuleException")
    roto.set_region("eu-west-1"); test.equal(c("DescribeConfigRules", {}).ConfigRules, {})
    roto.set_region("us-east-1"); roto.set_account("999999999999"); test.equal(c("DescribeConfigRules", {}).ConfigRules, {})
    roto.set_account(account)
    local recorder = {name = "default", roleARN = "arn:aws:iam::" .. account .. ":role/config"}
    c("PutConfigurationRecorder", {ConfigurationRecorder = recorder})
    test.equal(c("DescribeConfigurationRecorders", {}).ConfigurationRecorders, {recorder})
    c("StartConfigurationRecorder", {ConfigurationRecorderName = "default"})
    test.equal(c("DescribeConfigurationRecorderStatus", {}).ConfigurationRecordersStatus[1].recording, true)
    c("StopConfigurationRecorder", {ConfigurationRecorderName = "default"})
    test.equal(c("DescribeConfigurationRecorderStatus", {}).ConfigurationRecordersStatus[1].recording, false)
    errorCode("config", "StartConfigurationRecorder", {ConfigurationRecorderName = "missing"}, "NoSuchConfigurationRecorderException")
    c("PutDeliveryChannel", {DeliveryChannel = {name = "default", s3BucketName = "config-data"}})
    test.equal(c("DescribeDeliveryChannels", {}).DeliveryChannels[1].s3BucketName, "config-data")
    errorCode("config", "DescribeDeliveryChannels", {DeliveryChannelNames = {"missing"}}, "NoSuchDeliveryChannelException")
    local delete = table.clone(rule); delete.ConfigRuleName = "delete"
    c("PutConfigRule", {ConfigRule = delete}); c("DeleteConfigRule", {ConfigRuleName = "delete"})
    errorCode("config", "DeleteConfigRule", {ConfigRuleName = "missing"}, "NoSuchConfigRuleException")
    test.equal(c("DescribeDeliveryChannelStatus", {}), {DeliveryChannelsStatus = {{name = "default"}}})
    test.equal(c("DescribeComplianceByConfigRule", {}).ComplianceByConfigRules[1].Compliance.ComplianceType, "INSUFFICIENT_DATA")
    test.equal(c("GetComplianceDetailsByConfigRule", {ConfigRuleName = "versioning"}), {EvaluationResults = roto.array({})})
    test.equal(c("DescribeConfigRuleEvaluationStatus", {}).ConfigRulesEvaluationStatus[1].FirstEvaluationStarted, false)
    errorCode("config", "StartConfigRulesEvaluation", {ConfigRuleNames = {"versioning"}}, "NotImplemented")
end)

test.case("Budgets notifications, subscribers, tags and rollback", function()
    local function c(op, input) return call("budgets", op, input) end
    local scope = {AccountId = account, BudgetName = "budget"}
    local budget = {BudgetName = "budget", BudgetType = "COST", TimeUnit = "MONTHLY",
        BudgetLimit = {Amount = "500", Unit = "USD"},
        FilterExpression = {Dimensions = {Key = "SERVICE", Values = {"Amazon S3"}}}, Metrics = {"UnblendedCost"}}
    local notification = {NotificationType = "ACTUAL", ComparisonOperator = "GREATER_THAN", Threshold = 90}
    local sub = {SubscriptionType = "EMAIL", Address = "one@example.com"}
    c("CreateBudget", {AccountId = account, Budget = budget, NotificationsWithSubscribers = {{Notification = notification, Subscribers = {sub}}}})
    errorCode("budgets", "CreateBudget", {AccountId = account, Budget = budget}, "DuplicateRecordException")
    local value = c("DescribeBudget", scope).Budget
    assert(value.FilterExpression == nil and value.Metrics == nil)
    assert(c("DescribeBudget", {AccountId = account, BudgetName = "budget", ShowFilterExpression = true}).Budget.FilterExpression)
    test.equal(value.CalculatedSpend, {ActualSpend = {Amount = "0", Unit = "USD"}})
    test.equal(value.TimePeriod.Start, 1735689600)
    test.equal(value.TimePeriod.End, 3706473600)
    local updated = table.clone(budget); updated.BudgetLimit = {Amount = "600", Unit = "USD"}
    c("UpdateBudget", {AccountId = account, NewBudget = updated})
    roto.set_region("eu-west-1"); test.equal(c("DescribeBudget", scope).Budget.BudgetLimit.Amount, "600")
    roto.set_region("us-east-1"); roto.set_account("999999999999")
    errorCode("budgets", "DescribeBudget", {AccountId = "999999999999", BudgetName = "budget"}, "NotFoundException")
    roto.set_account(account)
    local second = {NotificationType = "ACTUAL", ComparisonOperator = "GREATER_THAN", Threshold = 50, ThresholdType = "PERCENTAGE"}
    c("CreateNotification", {AccountId = account, BudgetName = "budget", Notification = second, Subscribers = {sub}})
    errorCode("budgets", "CreateNotification", {AccountId = account, BudgetName = "budget", Notification = notification, Subscribers = {sub}}, "DuplicateRecordException")
    local new = table.clone(second); new.Threshold = 60
    c("UpdateNotification", {AccountId = account, BudgetName = "budget", OldNotification = second, NewNotification = new})
    errorCode("budgets", "UpdateNotification", {AccountId = account, BudgetName = "budget", OldNotification = new, NewNotification = notification}, "DuplicateRecordException")
    test.equal(#c("DescribeNotificationsForBudget", scope).Notifications, 2)
    local page = c("DescribeNotificationsForBudget", {AccountId = account, BudgetName = "budget", MaxResults = 1})
    assert(page.NextToken)
    test.equal(#c("DescribeNotificationsForBudget", {AccountId = account, BudgetName = "budget", MaxResults = 1, NextToken = page.NextToken}).Notifications, 1)
    c("DeleteNotification", {AccountId = account, BudgetName = "budget", Notification = new})
    test.equal(c("DescribeNotificationsForBudget", scope).Notifications[1].ThresholdType, "PERCENTAGE")
    local other = {SubscriptionType = "EMAIL", Address = "two@example.com"}
    local subInput = {AccountId = account, BudgetName = "budget", Notification = notification, Subscriber = other}
    c("CreateSubscriber", subInput)
    errorCode("budgets", "CreateSubscriber", subInput, "DuplicateRecordException")
    local renamed = {SubscriptionType = "EMAIL", Address = "three@example.com"}
    c("UpdateSubscriber", {AccountId = account, BudgetName = "budget", Notification = notification, OldSubscriber = other, NewSubscriber = renamed})
    test.equal(c("DescribeSubscribersForNotification", {AccountId = account, BudgetName = "budget", Notification = notification}).Subscribers, {sub, renamed})
    c("DeleteSubscriber", {AccountId = account, BudgetName = "budget", Notification = notification, Subscriber = renamed})
    c("DeleteSubscriber", {AccountId = account, BudgetName = "budget", Notification = notification, Subscriber = sub})
    test.equal(c("DescribeNotificationsForBudget", scope).Notifications, {})
    local arn = "arn:aws:budgets::" .. account .. ":budget/budget"
    c("TagResource", {ResourceARN = arn, ResourceTags = {{Key = "env", Value = "dev"}}})
    c("TagResource", {ResourceARN = arn, ResourceTags = {{Key = "env", Value = "prod"}}})
    test.equal(c("ListTagsForResource", {ResourceARN = arn}).ResourceTags, {{Key = "env", Value = "prod"}})
    c("UntagResource", {ResourceARN = arn, ResourceTagKeys = {"env"}})
    test.equal(c("ListTagsForResource", {ResourceARN = arn}).ResourceTags, {})
    errorCode("budgets", "TagResource", {ResourceARN = "arn:aws:budgets::999999999999:budget/budget", ResourceTags = roto.array({})}, "NotFoundException")
    local rollback = table.clone(budget); rollback.BudgetName = "rollback"
    errorCode("budgets", "CreateBudget", {AccountId = account, Budget = rollback,
        NotificationsWithSubscribers = {{Notification = notification, Subscribers = {sub}}, {Notification = notification, Subscribers = roto.array({})}}}, "InvalidParameterException")
    errorCode("budgets", "DescribeBudget", {AccountId = account, BudgetName = "rollback"}, "NotFoundException")
    local delete = table.clone(budget); delete.BudgetName = "delete"
    c("CreateBudget", {AccountId = account, Budget = delete}); c("DeleteBudget", {AccountId = account, BudgetName = "delete"})
    test.equal(#c("DescribeBudgets", {AccountId = account}).Budgets, 1)
end)

test.case("Cloud Map namespaces, services, instances and health", function()
    local function c(op, input) return call("servicediscovery", op, input) end
    local op = c("CreateHttpNamespace", {Name = "apps.local", Tags = {{Key = "env", Value = "dev"}}}).OperationId
    local operation = c("GetOperation", {OperationId = op}).Operation
    test.equal(operation.Status, "SUCCESS")
    local nid = operation.Targets.NAMESPACE
    test.equal(c("GetNamespace", {Id = nid}).Namespace.Properties, {HttpProperties = {HttpName = "apps.local"}})
    errorCode("servicediscovery", "CreateHttpNamespace", {Name = "apps.local"}, "NamespaceAlreadyExists")
    roto.set_region("eu-west-1"); errorCode("servicediscovery", "GetNamespace", {Id = nid}, "NamespaceNotFound")
    roto.set_region("us-east-1"); roto.set_account("999999999999"); errorCode("servicediscovery", "GetNamespace", {Id = nid}, "NamespaceNotFound")
    roto.set_account(account)
    c("UpdateHttpNamespace", {Id = nid, Namespace = {Description = "updated"}})
    test.equal(c("GetNamespace", {Id = nid}).Namespace.Description, "updated")
    local service = c("CreateService", {Name = "api", NamespaceId = nid, HealthCheckCustomConfig = {FailureThreshold = 1}}).Service
    local sid = service.Id
    test.equal(c("GetNamespace", {Id = nid}).Namespace.ServiceCount, 1)
    errorCode("servicediscovery", "DeleteNamespace", {Id = nid}, "ResourceInUse")
    c("UpdateService", {Id = sid, Service = {Description = "updated"}})
    test.equal(c("GetService", {Id = sid}).Service.Description, "updated")
    c("RegisterInstance", {ServiceId = sid, InstanceId = "one", Attributes = {version = "v1"}})
    c("RegisterInstance", {ServiceId = sid, InstanceId = "two", Attributes = {version = "v2"}})
    test.equal(c("GetService", {Id = sid}).Service.InstanceCount, 2)
    test.equal(c("GetInstance", {ServiceId = sid, InstanceId = "one"}), {Instance = {Id = "one", Attributes = {version = "v1"}}})
    local query = {NamespaceName = "apps.local", ServiceName = "api"}
    test.equal(#c("DiscoverInstances", query).Instances, 2)
    test.equal(c("DiscoverInstances", {NamespaceName = "apps.local", ServiceName = "api", QueryParameters = {version = "v1"}}).Instances[1].InstanceId, "one")
    local revision = c("DiscoverInstancesRevision", query).InstancesRevision
    c("UpdateInstanceCustomHealthStatus", {ServiceId = sid, InstanceId = "one", Status = "UNHEALTHY"})
    assert(c("DiscoverInstancesRevision", query).InstancesRevision > revision)
    test.equal(#c("DiscoverInstances", query).Instances, 1)
    test.equal(#c("DiscoverInstances", {NamespaceName = "apps.local", ServiceName = "api", HealthStatus = "ALL"}).Instances, 2)
    test.equal(c("DiscoverInstances", {NamespaceName = "apps.local", ServiceName = "api", HealthStatus = "UNHEALTHY"}).Instances[1].InstanceId, "one")
    test.equal(c("GetInstancesHealthStatus", {ServiceId = sid, Instances = {"one", "missing"}}).Status, {one = "UNHEALTHY", missing = "UNKNOWN"})
    errorCode("servicediscovery", "UpdateInstanceCustomHealthStatus", {ServiceId = sid, InstanceId = "one", Status = "bad"}, "InvalidInput")
    c("UpdateServiceAttributes", {ServiceId = sid, Attributes = {release = "v1", team = "platform"}})
    c("DeleteServiceAttributes", {ServiceId = sid, Attributes = {"team"}})
    test.equal(c("GetServiceAttributes", {ServiceId = sid}).ServiceAttributes.Attributes, {release = "v1"})
    c("TagResource", {ResourceARN = service.Arn, Tags = {{Key = "env", Value = "dev"}}})
    test.equal(c("ListTagsForResource", {ResourceARN = service.Arn}).Tags, {{Key = "env", Value = "dev"}})
    c("UntagResource", {ResourceARN = service.Arn, TagKeys = {"env"}})
    test.equal(c("ListTagsForResource", {ResourceARN = service.Arn}).Tags, {})
    test.equal(#c("ListServices", {Filters = {{Name = "NAMESPACE_ID", Values = {nid}}}}).Services, 1)
    local page = c("ListInstances", {ServiceId = sid, MaxResults = 1})
    test.equal(#page.Instances, 1); assert(page.NextToken)
    assert(c("ListInstances", {ServiceId = sid, MaxResults = 1, NextToken = page.NextToken}).NextToken == nil)
    test.equal(c("ListNamespaces", {Filters = {{Name = "TYPE", Values = {"DNS_PUBLIC"}}}}).Namespaces, {})
    page = c("ListOperations", {MaxResults = 1, Filters = {{Name = "STATUS", Values = {"SUCCESS"}}}})
    test.equal(#page.Operations, 1); assert(page.NextToken)
    assert(page.Operations[1].Id and page.Operations[1].Status)
    errorCode("servicediscovery", "DeleteService", {Id = sid}, "ResourceInUse")
    for _, iid in {"one", "two"} do c("DeregisterInstance", {ServiceId = sid, InstanceId = iid}) end
    c("DeleteService", {Id = sid}); c("DeleteNamespace", {Id = nid})
    errorCode("servicediscovery", "CreatePrivateDnsNamespace", {Name = "dns.local", Vpc = "vpc-demo"}, "NotImplemented")
    op = c("CreateHttpNamespace", {Name = "persist.local"}).OperationId
    nid = c("GetOperation", {OperationId = op}).Operation.Targets.NAMESPACE
    sid = c("CreateService", {Name = "api", NamespaceId = nid}).Service.Id
    c("RegisterInstance", {ServiceId = sid, InstanceId = "persist", Attributes = {version = "v1"}})
end)
