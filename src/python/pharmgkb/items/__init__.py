# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

import process

class PharmgkbItem(Item):
    pass

class GeneDrugPairItem(Item):
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
    primary_key = set([ 'drug_name', 'gene_name', 'haplotype_name1', 'haplotype_name2' ])
    # mysql fields
    drug_name = Field()
    implications = Field()
    recommendation = Field()
    classification = Field()
    diplotype_egs = Field()
    # non-mysql fields (needed to populate genotype_drug_recommendation)
    gene_name = Field()
    haplotype_name1 = Field()
    haplotype_name2 = Field()

class gene_phenotype_drug_recommendation(Item):
    primary_key = set([ 'gene_name', 'phenotype_name', 'drug_recommendation_id' ])
    gene_name = Field()
    phenotype_name = Field()
    # drug_recommendation_id = Field()

class gene_haplotype_variant(Item):
    primary_key = set([ 'gene_name', 'haplotype_name', 'snp_id', 'allele' ])
    gene_name = Field()
    haplotype_name = Field()
    snp_id = Field()
    allele = Field()

class genotype_phenotype(Item):
    primary_key = set([ 'gene_name', 'haplotype_name1', 'haplotype_name2' ])
    gene_name = Field()
    haplotype_name1 = Field()
    haplotype_name2 = Field()
    phenotype_name = Field()
    # meta-data that we don't want in the csv
    # Phenotype (Genotype)
    phenotype_genotype = Field()

class genotype_drug_recommendation(Item):
    primary_key = set([ 'gene_name', 'haplotype_name1', 'haplotype_name2', 'drug_name' ])
    gene_name = Field()
    haplotype_name1 = Field()
    haplotype_name2 = Field()
    # drug_recommendation_id = Field()
    # non-mysql fields (needed to resolve drug_recommendation_id mappings)
    drug_name = Field()

# end of mysql tables

class unused_genotype_data(Item):
    source = Field()
    values = Field()

class gene_haplotype_phenotype(Item):
    gene_name = Field()
    haplotype_name = Field()
    phenotype_name = Field()

def copy_item_fields(item, to):
    for k in item.fields.keys():
        try:
            to[k] = item[k]
        except KeyError:
            # item doesn't have k set, so don't set it in to either
            pass
    return to
