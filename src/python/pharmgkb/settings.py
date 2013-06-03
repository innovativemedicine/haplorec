import os

# Scrapy settings for pharmgkb project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'pharmgkb'

SPIDER_MODULES = ['pharmgkb.spiders']
NEWSPIDER_MODULE = 'pharmgkb.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'pharmgkb (+http://www.yourdomain.com)'

# use a delay to prevent hitting the pharmgkb server too hard
DOWNLOAD_DELAY = 1 

ITEM_PIPELINES = [
        'pharmgkb.pipelines.CsvPipeline',
]
CSV_OUTPUT_DIR = os.environ.get('CSV_OUTPUT_DIR', '.')

HTTPCACHE_ENABLED = True
HTTPCACHE_DIR = 'cache'
