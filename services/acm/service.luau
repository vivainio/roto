--!strict
-- Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
-- Adapted from MiniStack ACM. Requested certificates are synthetic; no X.509 engine.
type Tag = { Key: string, Value: string }
type Validation = {
    DomainName: string, ValidationMethod: string, ValidationStatus: string,
    ResourceRecord: { Name: string, Type: string, Value: string },
}
-- The stored document. `PemBody`, `PemChain` and `Tags` are withheld from some responses.
type Certificate = {
    CertificateArn: string, DomainName: string, SubjectAlternativeNames: { string },
    Status: string, Type: string, KeyAlgorithm: string, SignatureAlgorithm: string,
    InUseBy: { string }, CreatedAt: number, IssuedAt: number, NotBefore: number, NotAfter: number,
    DomainValidationOptions: { Validation }, Options: { [string]: any },
    Tags: { Tag }?, PemBody: string?, PemChain: string?,
}
-- Request fields are untrusted JSON, so each is `any` and validated where it is used. The types
-- list which fields exist, so a misspelled one is an error.
type CertificateRef = { CertificateArn: any }
type RequestCertificateInput = { DomainName: any, ValidationMethod: any, SubjectAlternativeNames: any, Options: any, Tags: any }
type ListCertificatesInput = { CertificateStatuses: any, MaxItems: any, NextToken: any }
type ImportCertificateInput = { Certificate: any, PrivateKey: any, CertificateArn: any, CertificateChain: any, Tags: any }
type TagsInput = { CertificateArn: any, Tags: any }
type OptionsInput = { CertificateArn: any, Options: any }
local ops = {}
local code = "InvalidParameterException"
local function ns(ctx: Ctx): string return roto.scope(ctx,"certificates") end
local function new_arn(ctx: Ctx): string return "arn:aws:acm:" .. ctx.region .. ":" .. ctx.account_id .. ":certificate/" .. roto.uuid() end
local function get(ctx: Ctx, input: CertificateRef, db: Db): Certificate
    local arn = roto.required(input,"CertificateArn",code)
    local cert: Certificate? = db.get(ns(ctx),arn)
    if not cert then roto.error("ResourceNotFoundException","Certificate " .. arn .. " not found") end
    return cert :: Certificate -- roto.error above does not narrow the checker
end
local function validation(domain: string, method: string): Validation
    local base = domain:gsub("^%*%.","")
    return {DomainName=domain,ValidationMethod=method,ValidationStatus="SUCCESS",
        ResourceRecord={Name="_acme-challenge." .. base .. ".",Type="CNAME",
            Value="fake-validation-" .. roto.sha256(base):sub(1,8) .. ".acm.amazonaws.com."}}
end
local function pem(value: any): string
    if value == nil then return "" end
    if type(value) ~= "string" then roto.error(code,"Certificate blobs must be strings") end
    if value:match("^%s*%-%-%-%-%-") then return value end
    local ok,decoded = pcall(roto.decode_base64,value)
    if not ok then roto.error(code,"Invalid base64 certificate blob") end
    return decoded
end
function ops.RequestCertificate(ctx: Ctx, input: RequestCertificateInput, db: Db)
    local domain = roto.required(input,"DomainName",code)
    local method = input.ValidationMethod or "DNS"
    if method ~= "DNS" and method ~= "EMAIL" then roto.error(code,"Invalid ValidationMethod") end
    local sans = roto.array()
    table.insert(sans,domain)
    for _, name in ipairs(roto.list(input.SubjectAlternativeNames or roto.array(),code,"SubjectAlternativeNames")) do
        if type(name) ~= "string" or name == "" then roto.error(code,"Invalid subject alternative name") end
        if not roto.contains(sans,name) then table.insert(sans,name) end
    end
    local arn,now = new_arn(ctx),os.time()
    local validations = roto.array()
    for _, name in ipairs(sans) do table.insert(validations,validation(name,method)) end
    local cert = {CertificateArn=arn,DomainName=domain,SubjectAlternativeNames=sans,
        Status="ISSUED",Type="AMAZON_ISSUED",KeyAlgorithm="RSA_2048",SignatureAlgorithm="SHA256WITHRSA",
        InUseBy=roto.array(),CreatedAt=now,IssuedAt=now,NotBefore=now,NotAfter=now+365*86400,
        DomainValidationOptions=validations,Options=input.Options or {},
        Tags=roto.tags(nil,input.Tags or roto.array(),code),
        PemBody="-----BEGIN CERTIFICATE-----\nAQIDBAUGBwgJCgsMDQ4PEA==\n-----END CERTIFICATE-----\n",PemChain=""}
    db.put(ns(ctx),arn,cert)
    return {CertificateArn=arn}
end
function ops.DescribeCertificate(ctx: Ctx, input: CertificateRef, db: Db)
    local cert = get(ctx,input,db)
    cert.PemBody,cert.PemChain,cert.Tags = nil,nil,nil
    return {Certificate=cert}
end
function ops.ListCertificates(ctx: Ctx, input: ListCertificatesInput, db: Db)
    local values = roto.array()
    for _, cert in ipairs(db.list(ns(ctx))) do
        if not input.CertificateStatuses or #input.CertificateStatuses == 0 or roto.contains(input.CertificateStatuses,cert.Status) then
            table.insert(values,{CertificateArn=cert.CertificateArn,DomainName=cert.DomainName,Status=cert.Status})
        end
    end
    return roto.page({MaxResults=input.MaxItems,NextToken=input.NextToken},values,"CertificateSummaryList",100,1000,code)
end
function ops.DeleteCertificate(ctx: Ctx, input: CertificateRef, db: Db)
    local cert = get(ctx,input,db)
    db.delete(ns(ctx),cert.CertificateArn)
    return {}
end
function ops.GetCertificate(ctx: Ctx, input: CertificateRef, db: Db)
    local cert = get(ctx,input,db)
    return {Certificate=cert.PemBody,CertificateChain=cert.PemChain}
end
function ops.ImportCertificate(ctx: Ctx, input: ImportCertificateInput, db: Db)
    -- Decode/validate the private key without retaining it in SQLite or responses.
    local body = pem(roto.required(input,"Certificate",code))
    pem(roto.required(input,"PrivateKey",code))
    local arn: string = input.CertificateArn or new_arn(ctx)
    local old = input.CertificateArn and get(ctx,input,db) or {}
    local domain = old.DomainName or "imported-cert-" .. (arn:match("certificate/(.+)") :: string):sub(1,8) .. ".invalid"
    local sans = old.SubjectAlternativeNames or roto.array()
    if #sans == 0 then table.insert(sans,domain) end
    local now = os.time()
    local cert = {CertificateArn=arn,DomainName=domain,SubjectAlternativeNames=sans,Status="ISSUED",Type="IMPORTED",
        KeyAlgorithm="RSA_2048",SignatureAlgorithm="SHA256WITHRSA",InUseBy=roto.array(),
        CreatedAt=old.CreatedAt or now,IssuedAt=now,NotBefore=now,NotAfter=now+365*86400,
        DomainValidationOptions=roto.array(),Options=old.Options or {},
        Tags=roto.tags(nil,input.Tags or old.Tags or roto.array(),code),PemBody=body,PemChain=pem(input.CertificateChain)}
    db.put(ns(ctx),arn,cert)
    return {CertificateArn=arn}
end
function ops.AddTagsToCertificate(ctx: Ctx, input: TagsInput, db: Db)
    local cert = get(ctx,input,db)
    cert.Tags = roto.tags(cert.Tags,input.Tags,code)
    db.put(ns(ctx),cert.CertificateArn,cert)
    return {}
end
function ops.RemoveTagsFromCertificate(ctx: Ctx, input: TagsInput, db: Db)
    local cert = get(ctx,input,db)
    local keys = roto.array()
    for _, tag in ipairs(roto.list(input.Tags,code,"Tags")) do table.insert(keys,roto.required(tag,"Key",code)) end
    cert.Tags = roto.untag(cert.Tags,keys,code)
    db.put(ns(ctx),cert.CertificateArn,cert)
    return {}
end
function ops.ListTagsForCertificate(ctx: Ctx, input: CertificateRef, db: Db) return {Tags=get(ctx,input,db).Tags} end
function ops.UpdateCertificateOptions(ctx: Ctx, input: OptionsInput, db: Db)
    local cert = get(ctx,input,db)
    cert.Options = roto.object(input.Options,code,"Options")
    db.put(ns(ctx),cert.CertificateArn,cert)
    return {}
end
-- Renewal and validation-email delivery need a certificate lifecycle/delivery engine.
return ops
