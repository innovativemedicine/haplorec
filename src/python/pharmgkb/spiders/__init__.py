"""
Define spiders for PharmGKB.  

Each page in the spider-crawl is handled by one spider.

A spider may produce zero or more items (of zero or more item types).
"""

def as_func(spider_class, *args, **kwargs):
    """
    Given a spider class, return a function for instantiating it with args/kwargs and calling its 
    parse method (on a supplied response).  In other words, call a spider class like a function 
    whose return value is that_spider.parse(response).
    """
    def run_spider(response):
        spider = spider_class(*args, **kwargs)
        return spider.parse(response)
    return run_spider
