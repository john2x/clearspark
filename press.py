
class BusinessWire:
  def _recent():
    ''' '''

class Press:
  # df get signals from parse
  # iterate over every url -> maybe hit google cache?
  # scrape
  def _recent():
    for signal in signals:
      for url in links:
        q.enqueue(Press()._scrape, url, source_domain, signal_name)

  def _scrape(url, source, domain, signal_name):
    html = requests.get(url).text
    if domain == "prnewswire.com":
      val = PRNewsWire()._recent_to_dict(html)
    elif domain == "newswire.ca":
      val = NewsWire()._recent_to_dict(html)
    elif domain == "marketwire.com":
      val = MarketWire()._recent_to_dict(html)
    elif domain == "businesswire.com":
      val = BusinessWire()._recent_to_dict(html)
    CompanyExtraInfoCrawl()._persist(val, "recent_press")
