# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.

def as_func(spider_class, *args):
    def run_spider(response):
        spider = spider_class(*args)
        return spider.parse(response)
    return run_spider
