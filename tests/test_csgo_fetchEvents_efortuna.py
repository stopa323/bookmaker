from lambdas.csgo.fetch_events.efortuna_main import extract_event_links

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
