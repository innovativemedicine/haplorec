# from scrapy.contrib.spiders.crawl import CrawlSpider, Rule
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
# from scrapy.spider import CrawlSpider
from scrapy.selector import HtmlXPathSelector
from pharmgkb import items, parsers, spiders
from pharmgkb.spiders import Gene, as_func

class GeneDrugPairSpider(CrawlSpider):
    """
    Crawl starting at a PharmGKB gene-drug pair page (http://www.pharmgkb.org/page/cpicGeneDrugPairs).
    
    This spider extracts nothing.

    This spider crawls out to:

    * :class:`.GeneSpider`'s for each gene on this page (which has a corresponding drug associated 
      with it)
    """
    name = "GeneDrugPair"
    # allowed_domains = ["pharmgkb.org"]
    rules = (
        Rule(SgmlLinkExtractor(restrict_xpaths='//div[@id="cpicGeneDrugPairsContent"]/table',
                               allow=( r'/gene/', )), 
             callback=as_func(Gene.GeneSpider)),
    )

    def __init__(self, start_url='http://www.pharmgkb.org/page/cpicGeneDrugPairs', *a, **kw):
        self.start_urls = (start_url,)
        super(GeneDrugPairSpider, self).__init__(*a, **kw)
