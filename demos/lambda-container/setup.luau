--!strict
roto.lambda.fn("container-echo", {
  timeout = 30,
  environment = {
    AWS_ACCESS_KEY_ID = "testing",
    AWS_SECRET_ACCESS_KEY = "testing",
  },
  executor = {container = "roto-lambda-container:latest"},
})
