roto.lambda.function_("warm-counter", {
  timeout = 30,
  executor = {container = "roto-lambda-warm:latest", warm = true},
})
roto.lambda.function_("cold-counter", {
  timeout = 30,
  executor = {container = "roto-lambda-warm:latest", warm = false},
})
