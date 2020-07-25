resource "aws_apigatewayv2_api" "this" {
  name          = "bookmaker-http-api-gw"
  protocol_type = "HTTP"
}