from scrapy import signals
from scrapy.exceptions import DropItem
from scrapy.contrib.exporter import CsvItemExporter, JsonLinesItemExporter
import items
import settings

import collections
import os
import os.path
import errno
import fileinput
import sys
import csv

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

output_dir = settings.CSV_OUTPUT_DIR

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
                exporter = JsonLinesItemExporter(open(_class_to_file(item.__class__), 'w+b'))
            else:
                exporter = CsvItemExporter(open(_class_to_file(item.__class__), 'w+b'))
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

def _filepath(filename):
    try:
        os.makedirs(output_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return os.path.join(output_dir, filename)

def _class_to_file(c):
    if item.__class__ == items.unused_genotype_data:
        return _filepath('{}.json'.format(item.__class__.__name__))
    else:
        return _filepath('{}.csv'.format(item.__class__.__name__)),
def _file_to_class(filename):
    classname = os.path.splitext(os.path.basename(filename))[0]
    return getattr(items, classname)

# post-processing of generated *.csv files (i.e. processing the depends on having all the scraped data to 
# perform, such as collapsing lines would otherwise cause duplicate primary-key errors upon insertion into mysql)

def dup_map(dicts, key):
    d = collections.defaultdict(list)
    for dict in dicts:
        d[tuple(dict[k] for k in key)].append(dict)
    return d

def _default_collapse(key_fields, non_key_fields, k, rows):
    row = {}
    for field, value in zip(key_fields, k):
        row[field] = value
    for field in non_key_fields:
        row[field] = '. '.join([r[field] for r in rows])
    return row

def collapse_by_key(input_file, output, collapse=_default_collapse):
    input = fileinput.input([input_file])
    item_class = _file_to_class(input_file)
    reader = csv.DictReader(input, delimiter=',')
    key_fields = item_class.primary_key
    non_key_fields = set([x for x in item_class.fields.keys() if x not in key_fields])
    writer = csv.DictWriter(output, reader.fieldnames, delimiter=',')
    writer.writeheader()
    for k, rows in dup_map(reader, key_fields).iteritems():
        row = collapse(key_fields, non_key_fields, k, rows)
        writer.writerow(row)
    input.close()


