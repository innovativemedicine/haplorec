# from scrapy.contrib.spiders.crawl import CrawlSpider, Rule
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
# from scrapy.spider import CrawlSpider
from scrapy.selector import HtmlXPathSelector
from pharmgkb import items, parsers, spiders

class GeneDrugPairSpider(CrawlSpider):
    name = "GeneDrugPair"
    # allowed_domains = ["pharmgkb.org"]
    rules = (
        Rule(SgmlLinkExtractor(restrict_xpaths='//div[@id="cpicGeneDrugPairsContent"]/table',
                               allow=( r'/gene/', )), 
             callback=spiders.as_func(spiders.GeneSpider)),
    )

    def __init__(self, start_url='http://www.pharmgkb.org/page/cpicGeneDrugPairs', *a, **kw):
        self.start_urls = (start_url,)
        super(GeneDrugPairSpider, self).__init__(*a, **kw)
