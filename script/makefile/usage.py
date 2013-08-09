#!/usr/bin/env python
from os import environ as e
import textwrap

def main():

    width = 90
    target_help = [ 
        ( 'all', """
            build the haplorec schema
        """ ),
        ( 'scrape', """
            scrape data from http://www.pharmgkb.org/page/cpicGeneDrugPairs, placing them in 
            tmp/scrapy/*.csv
        """ ),
        ( 'dbfiles', """
            post-process scraped data into files suitable for LOAD DATA INFILE into PharmGKB data 
            tables, placing them in tmp/scrapy/mysql/*.csv
        """ ),
        ( 'load_haplorec', """
            load the haplorec schema files in tmp/scrapy/mysql/*.csv into $HAPLOREC_DB_NAME 
            (default: {HAPLOREC_DB_NAME})
        """.format(**e) ),
    ]

    help_wrapper = textwrap.TextWrapper(width=width, initial_indent="    ", subsequent_indent="    ")
    print 'Useful targets:\n'
    for target, help in target_help:
        print target + ':'
        print help_wrapper.fill(textwrap.dedent(help).replace('\n', '')), '\n'

if __name__ == '__main__':
    main()
