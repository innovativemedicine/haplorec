"""
Define scrapy items (http://doc.scrapy.org/en/0.18/topics/items.html).  In our case, items 
correspond to PharmGKB data tables defined in haplorec.  

In the case where tables use auto_increment fields to reference each other, additional fields are 
added to the referring tables to allow disambiguation during insertion.  See the load_haplorec 
target in scrapy_config.mk, which calls script/load_dsv.py to accomplish this.

An example of the case described above is where drug_recommendation.drug_recommendation_id is 
referenced by gene_phenotype_drug_recommendation and genotype_drug_recommendation.

See documentation in:
http://doc.scrapy.org/topics/items.html
"""

from scrapy.item import Item, Field

import process

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
    """
    Record any information that we do not currently consider, that is present in PharmGKB's "Look up 
    your guideline" drug recommendations.

    e.g. given a drug recommendation:

        Phenotype (Genotype):
        An individual carrying ...

        Metabolizer Status:
        Ultrarapid metabolizer ...

        Implications:
        Increased metabolism ...

        Recommendations (Strength: Optional):
        Consider alternative ...

        Uconsidered Datum:
        Some stuff

    We would record values['Unconsidered Datum'] = 'Some stuff'.
    """
    source = Field()
    values = Field()

def copy_item_fields(item, to):
    """
    Copy fields from one Item to the other. 
    """
    for k in item.fields.keys():
        try:
            to[k] = item[k]
        except KeyError:
            # item doesn't have k set, so don't set it in to either
            pass
    return to
