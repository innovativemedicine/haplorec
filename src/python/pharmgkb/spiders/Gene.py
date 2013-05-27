import scrapy
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http.request.form import FormRequest
from pharmgkb import items
from pharmgkb import parsers
from spiders import as_func
from parsers.text import parse

import itertools
import re
import json
import urlparse
import collections

def get_domain(url):
    p = urlparse.urlparse(url)
    return "{}://{}".format(p.scheme, p.netloc)

def _spider_request_callback(spider, response):
    return spider.parse(response)
def spider_request(spider_class, response, callback=_spider_request_callback, **kwargs):
    base_url = get_domain(response.url)
    spider = spider_class(base_url, **kwargs)
    return spider.request(base_url, lambda r: callback(spider, r), **kwargs)

def pairs(xs, key=lambda pair: pair):
    return [tuple(sorted(pair, key=key)) for pair in itertools.combinations_with_replacement(xs, 2)]

class GeneSpider(BaseSpider):
    name = "Gene"
    # allowed_domains = ["pharmgkb.org"]

    def __init__(self, start_url=None):
        self.start_urls = (start_url,)

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        gene_name = re.search(r'^(.*)\s*\[PharmGKB\]$', hxs.select('//title/text()')[0].extract()).group(1).rstrip()
        t_xs = hxs.select('//div[@id="tabHaplotypes"]/article[@class="HaplotypeSet"]/*/table')
        if t_xs == []:
            # this gene has no "Haplotypes" tab on its page, so skip it
            return
        t = parsers.table(t_xs[0])
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

        base_url = get_domain(response.url)
        annotation_ids = hxs.select('//script[contains(@type, "javascript") and contains(text(), "popPickers")]/text()').re(r"popPickers\('#edg(\d+)','\d+'\);")
        for annotation_id in annotation_ids:
            yield spider_request(GeneHaplotypePhenotypeSpider, response,
                    annotation_id=annotation_id)

class FormRequestSpider(object):
    def __init__(self):
        self.formdata = {}

    def request(self, base_url, callback, **kwargs):
        for name, attr in self.formdata_fields:
            self.formdata[name] = kwargs[attr]
        return FormRequest(base_url + self.url, formdata=self.formdata, callback=callback)
        
    def start_requests(self):
        return [self.request(self.base_url, self.parse, **self.kwargs)]

    def parse(self, response):
        return self.parse_form_response(response, **self.kwargs)

    def error(self, msg):
        self.log(msg + "\nForm data: {formdata}".format(formdata=self.formdata), level=scrapy.log.ERROR)

def quadratic_formula(a, b, c):
    import math
    return (( -b + math.sqrt(b**2 - 4*a*c) ) / 2*a,
            ( -b - math.sqrt(b**2 - 4*a*c) ) / 2*a)

class GeneHaplotypePhenotypeException(Exception):
    pass

class GeneHaplotypePhenotypeSpider(FormRequestSpider, BaseSpider):
    name = "GeneHaplotypes"
    url = '/views/ajaxGuidelinePickerData.action'
    # allowed_domains = ["pharmgkb.org"]

    def __init__(self, base_url='http://www.pharmgkb.org', **kwargs):
        FormRequestSpider.__init__(self)
        self.base_url = base_url
        self.start_urls = (self.base_url + self.url,)
        self.kwargs = kwargs
        self.formdata_fields = [
            ('annotationId', 'annotation_id'),
        ]

    def parse_form_response(self, response, annotation_id=None):
        result = None
        try:
            result = json.loads(response.body)
        except ValueError:
            # empty response.body
            return
        gene_name = None
        haplotype_names = []
        haplotype_ids = []
        for r in result['results']:
            if 'gene' in r and 'haps' in r: 
                gene_name = r['gene']
                haplotype_names = [haplotype['name'] for haplotype in r['haps']]
                haplotype_ids = [haplotype['value'] for haplotype in r['haps']]
                for haplotype1, haplotype2 in [sorted(genotype, key=lambda haplotype: haplotype[0]) for genotype in itertools.combinations_with_replacement(itertools.izip(haplotype_names, haplotype_ids), 2)]:
                    haplotype_name1, haplotype_id1 = haplotype1
                    haplotype_name2, haplotype_id2 = haplotype2
                    yield spider_request(GenotypeSpider, response, 
                        haplotype_name1=haplotype_name1,
                        haplotype_name2=haplotype_name2,
                        haplotype_id1=haplotype_id1,
                        haplotype_id2=haplotype_id2,
                        gene_name=gene_name,
                        annotation_id=annotation_id)

class GenotypeSpider(FormRequestSpider, BaseSpider):
    max_genotype_requests = 100
    name = "Genotype"
    url = '/views/alleleGuidelines.action'
    # allowed_domains = ["pharmgkb.org"]

    def __init__(self, base_url='http://www.pharmgkb.org', **kwargs):
        FormRequestSpider.__init__(self)
        self.base_url = base_url
        self.start_urls = (self.base_url + self.url,)
        self.kwargs = kwargs
        self.formdata_fields = [
            ('annotationId', 'annotation_id'),
            ('allele1', 'haplotype_id1'),
            ('allele2', 'haplotype_id2'),
            ('location', 'gene_name'),
        ]

    def parse_form_response(self, response, haplotype_name1=None, haplotype_name2=None, haplotype_id1=None, haplotype_id2=None, gene_name=None, annotation_id=None):
        hxs = HtmlXPathSelector(response)

        if len(hxs.select('//text()').re('This guideline does not contain recommendations')) != 0:
            return

        drug_recommendation = items.drug_recommendation()
        genotype_phenotype = items.genotype_phenotype(
            haplotype_name1=haplotype_name1,
            haplotype_name2=haplotype_name2,
            gene_name=gene_name,
        )

        def strip_title(t):
            t = t.strip()
            m = re.search(r'^Recommendations\s*\(Strength:\s*([^)]*)\)', t)
            if m:
                return ('Recommendations', m.group(1))
            return t

        title_value = dict(itertools.izip([strip_title(t) for t in hxs.select('//dt/*/text()').extract()],
                           [' '.join(h.select('(* | */*)/text()').extract()) for h in hxs.select('//dd')]))

        phenotype_exceptions = { 
            'CYP2C19': 'Metabolizer Status',
            'CYP2D6': 'Metabolizer Status',
        }

        unused_genotype_data = items.unused_genotype_data(values={}, source={
            'gene_name': gene_name,
            'annotation_id': annotation_id,
            'haplotype_id1': haplotype_id1,
            'haplotype_id2': haplotype_id2,
        })
        for title, value in title_value.iteritems():
            if type(title) is tuple and title[0] == 'Recommendations':
                drug_recommendation['classification'] = title[1]               
                drug_recommendation['recommendation'] = value
            elif title == 'Phenotype (Genotype)' and gene_name not in phenotype_exceptions:
                genotype_phenotype['phenotype_name'] = value
            elif title == 'Implications':
                drug_recommendation['implications'] = value
            elif gene_name in phenotype_exceptions and title == phenotype_exceptions[gene_name]:
                genotype_phenotype['phenotype_name'] = value
            else:
                unused_genotype_data['values'][title] = value
            if title == 'Phenotype (Genotype)':
                genotype_phenotype['phenotype_genotype'] = value

        yield genotype_phenotype
        yield drug_recommendation
        if unused_genotype_data['values'] != {}:
            yield unused_genotype_data
