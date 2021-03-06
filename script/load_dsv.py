#!/usr/bin/env python
# non-standard libs (download with pip)
import oursql
from funcparserlib.parser import some, a, many, skip, finished, maybe

import fileinput
import re
import os.path
import textwrap
import collections
import csv
import itertools

import argparse
import argparsers

def main():
    description = """
        Given a list of dsv files, insert the files into their respective tables (files are named 
        <table_name>.<extension>), maintaining foreign key constraints. 
        
        For auto_increment tables T referenced by other tables R_i, we require a set of keys K_i in 
        both T and R_i that can be used to resolve generated ids from T to use in R_i.
        e.g. 
        T   = CREATE TABLE ( T_id auto_increment, x, y ) 
        R_1 = CREATE TABLE ( z, T_id                  ) 
        K_1 = x
        
        == T.dsv ==
        x  y
        x1 y1
        x2 y2

        == R_1.dsv ==
        z  x
        z1 x1

        # load_dsv.py T.dsv R_1.dsv --ignore R_1.x --map "R_1: x => T"
        # after insertion..

        T:
        | T_id | x  | y  | 
        | 1    | x1 | y1 | 
        | 2    | x2 | y2 | 

        R_1:
        | z  | T_id | 
        | z1 | 1    | 
    """
    parser = argparsers.sql_parser(description=textwrap.dedent(description), formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('files', nargs='+')
    parser.add_argument('--map', nargs='*')
    parser.add_argument('--ignore', nargs='*')
    parser.add_argument('--delim', default=",")
    args = parser.parse_args()
    
    db = oursql.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            passwd=args.password,
            db=args.db)

    load_dsv(db, args.files, args.map, args.ignore, args.delim)

def load_dsv(db, files, mapping_strings=[], ignore_strings=[], delim=","):
    metadata = table_metadata(db)

    tables = [os.path.splitext(os.path.basename(f))[0] for f in files]

    for table, file in zip(tables, files):
        check_table(metadata, table, extra_info=", as specified in {file}".format(**locals()))

    mappings = [parse(mapping, m) for m in mapping_strings]

    table_to_file = {}
    for file in files:
        table = os.path.splitext(os.path.basename(file))[0]
        table_to_file[table] = file

    def get_columns(file):
        with csv_reader(file, delim=delim) as input:
            return input.fieldnames

    for m in mappings:
        check_mapping(metadata, m, 
            get_columns(table_to_file[m['auto_increment_table']]), 
            get_columns(table_to_file[m['table']]))

    # a mapping from an auto_increment table to all tables it is referenced by (and through which fields).
    # T -> [ (R, [f1, ..., fn]) ]
    refs = collections.defaultdict(list) 
    # a mapping from a referencing table to all the auto_increment tables it references and the fields through which is does so.
    # R -> [ (T, [f1, ..., fn]) ]
    fks = collections.defaultdict(list)
    for m in mappings:
        R = m['table']
        T = m['auto_increment_table']
        columns = m['columns']
        refs[T].append( (R, tuple(columns)) ) 
        fks[R].append( (T, tuple(columns)) )
    # a mapping from a referencing table, an auto_increment table it references, and field values through which the referencing happened, to an auto_increment value in T.
    # R, T, [v1, ..., vn] -> T.id
    fk_id = {}

    # (R, X) -> Bool
    ignore_field = collections.defaultdict(lambda: False)
    for s in ignore_strings:
        ignore_field[tuple(parse(ignore, s))] = True

    def rows_for_insert(file):
        with csv_reader(file, delim=delim) as input:
            unignored = [f for f in input.fieldnames if not ignore_field[(t, f)]]
            foreign_keys = [T + "_id" for T, t_fields in fks[t]]
            header = unignored + foreign_keys 
            # the first row is the header
            yield header 
            for row in input:
                fk_ids = [fk_id[(t, T, tuple(row[f] for f in t_fields))] for T, t_fields in fks[t]]
                yield [row[f] for f in unignored] + fk_ids

    cursor = db.cursor()
    for t, file in zip(tables, files):
        insertion_input = rows_for_insert(file)
        header = insertion_input.next()
        if 'auto_increment_field' in metadata[t]:
            ids = [id for id in insert_rows_with_ids(cursor, t, header, insertion_input)]
            with csv_reader(file, delim=delim) as input:
                # re-read the input file, but this time we know the lastrowid's from each line inserted
                for T_id, row in itertools.izip(ids, input):
                    for R, columns in refs[t]:
                        fk_id[(R, t, tuple(row[c] for c in columns))] = T_id
        else:
            insert_rows(cursor, t, header, insertion_input)

def _insert_query(table, header):
    return """
        INSERT INTO {table} ({column_str}) VALUES ({qmarks})
    """.format(
        table=table,
        column_str=comma_join(header), 
        qmarks=comma_join(len(header)*['?']))

def insert_rows(cursor, table, header, rows):
    return cursor.executemany(_insert_query(table, header), rows)

def insert_rows_with_ids(cursor, table, header, rows):
    """
    Insert rows into table, and also return the lastrowid's of each inserted row
    """
    for row in rows:
        cursor.execute(_insert_query(table, header), row)
        yield cursor.lastrowid

def merge_dict(d1, d2):
    return dict(d1.items() + d2.items())

def comma_join(xs):
    return ', '.join(xs)
    
def check_table(metadata, table, extra_info=''):
    if table not in metadata:
        raise RuntimeError(("no such table {table} exists" + extra_info).format(**locals()))

def check_mapping(metadata, m, T_columns, R_columns):
    """
    For a mapping:
        R: x_1, ..., x_n => T
    ensures:
        1. R and T are tables in the database
        2. T has an auto_increment key
        3. columns are in R and T
    """
    extra_info = ' in the mapping {m}'.format(**locals()) 
    R = m['table']
    T = m['auto_increment_table']
    columns = m['columns']
    # 1.
    check_table(metadata, R, extra_info=extra_info)
    check_table(metadata, T, extra_info=extra_info)
    # 2.
    if 'auto_increment_field' not in metadata[T]:
        raise RuntimeError(( "the table {T} has no auto_increment field but one is required" ).format(**locals()) + extra_info )
    # 3.
    if not set(columns).issubset(set(T_columns).intersection(R_columns)):
        raise RuntimeError("mapping columns must be a subset of both {R} and {T}".format(**locals()) + extra_info)

def table_metadata(db):
    """
    Return table meta-data.
    """
    cursor = db.cursor(oursql.DictCursor)
    tables = {}
    def get(dictionary, key, default=dict):
        """
        Return dictionary[key] or set a default value and return it.
        """
        if key not in dictionary:
            dictionary[key] = default()
        return dictionary[key]
    cursor.execute("""
        SELECT *
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
    """)
    for row in cursor:
        table = get(tables, row['TABLE_NAME'])
        if row['COLUMN_KEY'] == 'PRI' and re.search(r"auto_increment", row['EXTRA']):
            table['auto_increment_field'] = row['COLUMN_NAME']
        columns = get(table, 'columns', default=list)
        columns.append(row['COLUMN_NAME'])
    return tables

class csv_reader:
    def __init__(self, file, delim=","):
        self.file = file
        self.delim = delim
    def __enter__(self):
        self.input = fileinput.FileInput([self.file])
        self.reader = csv.DictReader(self.input, delimiter=self.delim)
        return self.reader
    def __exit__(self, *args):
        self.input.close()

# Parsing code for handling the --map option

def tokenize(string):
    """
    tokenize("R_1: x, => T") = ['R_1', ':', 'x', ',', '=>', 'T'] 
    """
    return [x for x in re.split(r"(:|,|\.)|\s+", string) if x is not None and x is not '']

def parse(parser, string):
    return parser.parse(tokenize(string))

def separated(parser, by):
    def append(parsed):
        first = parsed[0]
        rest = parsed[1]
        rest.insert(0, first)
        return rest
    return ( parser + many(by + parser) ) >> append

identifier = some(lambda x: re.match(r"[A-Za-z][A-Za-z0-9_]*", x))
"""
parse(mapping, "R_1: x, => T") 
= 
{ 
    'table'               : 'R_1',
    'columns'             : ['x'],
    'auto_increment_table' : 'T',
}
"""
mapping = ( 
    identifier + skip(a(':')) + separated(identifier, skip(a(','))) + skip(a('=>')) + identifier 
) >> (lambda result: {
    'table'               : result[0],
    'columns'             : result[1],
    'auto_increment_table' : result[2],
})

ignore = (
    identifier + skip(a('.')) + identifier
)

if __name__ == '__main__':
    main()
