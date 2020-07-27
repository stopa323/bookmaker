from pytest_mock import mocker

from lambdas.csgo.fetch_events.efortuna_main import (
    extract_event_links, send_urls_to_sqs)


HTML = '''
<tbody aria-live="polite" aria-relevant="all">
  <tr role="row">
    <td class="col-title" data-value="BIG" data-type="text" <div=""> 
      <a href="event0-url" data-id="MPL27389258" class="event-name"> BIG </a>
      <span class="event-meta"><span class="event-info-number">60217</span></span>
      <div class="event-icons"></div>
    </td>
    <td class="col-odds">
      <a href="#" class="odds-button" data-info="60217" data-id="86836073" data-value="3.75">
        <span class="odds-value">3.75</span>
      </a>
    </td>
  </tr>
  <tr role="row">
    <td class="col-title" data-value="Complexity" data-type="text" <div="">
      <a href="event1-url" data-id="MPL27389261" class="event-name"> Complexity </a>
      <span class="event-meta"><span class="event-info-number">60218</span></span>
      <div class="event-icons"></div>
    </td>
    <td class="col-odds">
      <a href="#" class="odds-button" data-info="60218" data-id="86836080" data-value="5.0">
        <span class="odds-value">5.00</span>
      </a>
    </td>
  </tr>
</tbody>
'''


def test_event_url_is_extracted():
    events_url = extract_event_links(HTML)

    assert ["event0-url", "event1-url"] == events_url


def test_message_per_event_is_sent_to_sqs(mocker):
    sqs_client = mocker.Mock()
    sqs_queue_url = "https://sqs.eu-west-1.amazonaws.com/2137/kek"
    urls = ["url0", "url1"]

    expected_msg_0 = {"QueueUrl": sqs_queue_url, "MessageBody": urls[0]}
    expected_msg_1 = {"QueueUrl": sqs_queue_url, "MessageBody": urls[1]}

    send_urls_to_sqs(sqs_client, sqs_queue_url, urls)

    assert 2 == sqs_client.send_message.call_count
    assert expected_msg_0 == sqs_client.mock_calls[0][2]
    assert expected_msg_1 == sqs_client.mock_calls[1][2]
