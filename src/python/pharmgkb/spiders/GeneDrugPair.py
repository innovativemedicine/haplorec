from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from pharmgkb import items
from pharmgkb import parsers

class GeneDrugPairSpider(BaseSpider):
    name = "GeneDrugPair"
    # allowed_domains = ["pharmgkb.org"]

    def __init__(self, start_url='http://www.pharmgkb.org/page/cpicGeneDrugPairs'):
        self.start_urls = (start_url,)

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        t = parsers.table(hxs.select('//div[@id="cpicGeneDrugPairsContent"]/table'))
        header = t.next()
        for row in t:
            genes = row[0].select('a[starts-with(@href, "/gene")]')
            drugs = row[0].select('a[starts-with(@href, "/drug")]')
            yield items.GeneDrugPairItem(
                    gene_drug_pairs = (
                        genes.select('text()').extract(), 
                        drugs.select('text()').extract(), 
                    ),
                    # status = ,
                    # author_contact = ,
                    # others_involved = ,
                    # publication_link = ,
                    # update = ,
                    )
