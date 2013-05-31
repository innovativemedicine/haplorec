from scrapy import signals
from scrapy.exceptions import DropItem
from scrapy.contrib.exporter import CsvItemExporter, JsonLinesItemExporter
import items

import collections

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

class CsvPipeline(object):
    def __init__(self):
        self.exporters = {}
        # (Item.__class__, item.values) => True
        self.items_seen = set()

    def get_exporter(self, item):
        exporter = None
        if item.__class__ in self.exporters:
            exporter = self.exporters[item.__class__]
        else:
            if item.__class__ == items.unused_genotype_data:
                exporter = JsonLinesItemExporter(open('{}.json'.format(item.__class__.__name__), 'w+b'))
            else:
                exporter = CsvItemExporter(open('{}.csv'.format(item.__class__.__name__), 'w+b'))
            self.exporters[item.__class__] = exporter
            exporter.start_exporting()
        return exporter

    def item_seen(self, item):
        return self.item_key(item) in self.items_seen

    def process_item(self, item, spider):
        if item.__class__ == items.unused_genotype_data:
            return item
        item_key = _item_key(item)
        if item_key not in self.items_seen:
            exporter = self.get_exporter(item)
            exporter.export_item(item)
            self.items_seen.add(item_key)
            return item
        raise DropItem("Duplicate item found: %s" % item)

def _item_key(item):
    return (item.__class__, tuple(sorted(item.items(), key=lambda name_value: name_value[0])))

def dup_map(dicts, key):
    d = collections.defaultdict(list)
    for dict in dicts:
        d[tuple(dict[k] for k in key)].append(dict)
    return d
