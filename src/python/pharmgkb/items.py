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

