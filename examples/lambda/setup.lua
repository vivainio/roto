roto.lambda.bind("process-upload", {
  command = {"python3", "handler.py"},
})

roto.lambda.bind("http-handler", {
  url = "http://127.0.0.1:8080/invoke",
})
