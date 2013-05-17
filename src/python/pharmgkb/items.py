# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class PharmgkbItem(Item):
    # define the fields for your item here like:
    # name = Field()
    pass

class GeneDrugPairItem(Item):
    # define the fields for your item here like:
    # name = Field()
    gene_drug_pairs = Field()
    # status = Field()
    # author_contact = Field()
    # others_involved = Field()
    # publication_link = Field()
    # update = Field()

class GeneItem(Item):
    snp_ids = Field()
    alleles = Field()

# mysql tables

class drug_recommendation(Item):
    drug_name = Field()
    implications = Field()
    recommendation = Field()
    classification = Field()
    diplotype_egs = Field()

class gene_phenotype_drug_recommendation(Item):
    gene_name = Field()
    phenotype_name = Field()
    # drug_recommendation_id = Field()

class gene_haplotype_variant(Item):
    gene_name = Field()
    haplotype_name = Field()
    snp_id = Field()
    allele = Field()

class genotype_phenotype(Item):
    gene_name = Field()
    haplotype_name1 = Field()
    haplotype_name2 = Field()
    phenotype_name = Field()

class genotype_drug_recommendation(Item):
    gene_name = Field()
    haplotype_name1 = Field()
    haplotype_name2 = Field()
    # drug_recommendation_id = Field()

