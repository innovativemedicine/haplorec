from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from pharmgkb import items
from pharmgkb import parsers

import itertools
import re

class GeneSpider(BaseSpider):
    name = "Gene"
    # allowed_domains = ["pharmgkb.org"]

    def __init__(self, start_url=None):
        self.start_urls = (start_url,)

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        gene_name = re.search(r'^(.*)\s*\[PharmGKB\]$', hxs.select('//title/text()')[0].extract()).group(1).rstrip()
        t = parsers.table(hxs.select('//div[@id="tabHaplotypes"]/article[@class="HaplotypeSet"]/*/table')[0])
        header = t.next()
        snp_ids = header.select('text() | a/text()')[1:].extract()
        alleles = {}
        for row in t:
            haplotype_name = row[0].select('a/text()')[0].extract()
            for allele, snp_id in itertools.izip([r.select('text()').extract()[0].strip() for r in row[1:]], snp_ids):
                yield items.gene_haplotype_variant(
                        gene_name=gene_name,
                        haplotype_name=haplotype_name,
                        snp_id=snp_id,
                        allele=allele,
                        )
