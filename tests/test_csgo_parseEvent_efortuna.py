from lambdas.csgo.parse_event.efortuna_main import (
    extract_urls_from_event, handler)


LAMBDA_EVENT_MESSAGE = {
  "Records": [
    {
      "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
      "receiptHandle": "MessageReceiptHandle",
      "body": "url1",
      "attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1523232000000",
        "SenderId": "123456789012",
        "ApproximateFirstReceiveTimestamp": "1523232000001"
      },
      "messageAttributes": {},
      "md5OfBody": "7b270e59b47ff90a553787216d55d91d",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:MyQueue",
      "awsRegion": "eu-west-1"
    }
  ]
}


def test_url_is_extracted_from_event():
    urls = extract_urls_from_event(LAMBDA_EVENT_MESSAGE)

    assert ["url1"] == urls


def test_():
    event = {"Records": [
        {"body": "/zaklady-bukmacherskie/esport-cs-go/MPL29936/good-game-pr-team-surkz-MPL27440227"},
    ]}
    response = handler(event, None)
    print(response)
