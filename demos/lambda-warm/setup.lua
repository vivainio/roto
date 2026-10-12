roto.lambda.fn("warm-counter", {
  timeout = 30,
  executor = {container = "roto-lambda-warm:latest", warm = true},
})
roto.lambda.fn("cold-counter", {
  timeout = 30,
  executor = {container = "roto-lambda-warm:latest", warm = false},
})
