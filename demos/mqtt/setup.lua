-- Demonstrate policy simulation using the decoded MQTT request dictionaries.
roto.intercept_request("mqtt", "Connect", function(request, context)
  assert(request.service == "mqtt" and request.operation == "Connect")
  assert(request.path == "/mqtt" and request.source_ip == "127.0.0.1")
  assert(request.connection_id and request.headers and context.request_id)
  local connect = request.connect
  assert(connect.protocol_version == 4 and connect.client_id)
  if connect.client_id == "denied" then
    return {return_code = 5} -- Not authorized.
  end
  if connect.client_id == "hook-error" then error("Injected connect hook failure") end
  if connect.client_id == "invalid-hook" then return {return_code = 99} end
  if connect.client_id == "acceptance" then
    assert(connect.username == "fake" and connect.password == "fake")
    assert(connect.password_base64 == "ZmFrZQ==")
    assert(connect.clean_session == false and connect.keep_alive == 1)
    assert(connect.will.topic == "demo/will" and connect.will.qos == 1)
    assert(connect.will.retain == false and connect.will.payload_base64 == "aWdub3JlZA==")
  end
  return nil -- Normal acceptance.
end)

roto.intercept_request("mqtt", "Subscribe", function(request, context)
  assert(request.connect.client_id and request.packet_id and context.account_id)
  local codes = {}
  for i, filter in ipairs(request.filters) do
    if filter.topic == "demo/private/#" then
      codes[i] = 128 -- Reject this filter.
    else
      codes[i] = filter.qos <= 1 and filter.qos or 128
    end
  end
  return {return_codes = roto.array(codes)}
end)
