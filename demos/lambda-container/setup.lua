roto.lambda.function_("container-echo", {
  timeout = 30,
  environment = {
    AWS_ACCESS_KEY_ID = "testing",
    AWS_SECRET_ACCESS_KEY = "testing",
  },
  executor = {rie = "roto-lambda-container:latest"},
})
