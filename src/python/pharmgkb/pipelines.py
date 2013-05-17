from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

class CsvPipeline(object):
    def __init__(self):
        self.exporters = {}

    def get_exporter(self, item):
        exporter = None
        if item.__class__ in self.exporters:
            exporter = self.exporters[item.__class__]
        else:
            exporter = CsvItemExporter(open('{}.csv'.format(item.__class__.__name__), 'w+b'))
            self.exporters[item.__class__] = exporter
            exporter.start_exporting()
        return exporter

    def process_item(self, item, spider):
        exporter = self.get_exporter(item)
        exporter.export_item(item)
        return item
