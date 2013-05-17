#!/usr/bin/env python
import MySQLdb
import fileinput
import re
import os.path

import argparsers

def main():
    parser = argparsers.sql_parser(description="Load .csv files output from the scrapy pipeline.  The expected format is:\n" +
            "column_name1,column_name2,...,column_namen\n" +
            "value1,value2,...,valuen\n")
    parser.add_argument('files', nargs='+')
    args = parser.parse_args()
    
    db = MySQLdb.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            passwd=args.password,
            db=args.db)
    c = db.cursor()

    for f in args.files:
        load_csv_file(db, c, f)

def load_csv_file(db, cursor, filename):
    table = re.search(r'(.*)\.[^.]+$', os.path.basename(filename)).group(1)
    input = fileinput.input(filename)     
    header = input.next().rstrip().split(',')
    columns_str = ', '.join(header)
    cursor.execute("""
    LOAD DATA LOCAL INFILE %s INTO TABLE {table} 
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    IGNORE 1 LINES
    ({columns_str})
    """.format(**locals()), (filename,))
    db.commit()

if __name__ == '__main__':
    main()
