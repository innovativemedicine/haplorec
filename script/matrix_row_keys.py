#!/usr/bin/env python
import argparse
import fileinput
import textwrap
import csv
import collections
from itertools import izip, ifilter

def main():
    description = textwrap.dedent("""
    Given a labelled 2D matrix of values, output the row keys that uniquely determine each row,
    where a row key is a set of row name / value pairs.

    For example:
       x1 x2
    y1 1  2
    y2 1  3

    Outputs:
    {x2="2"} -> y1
    {x2="3"} -> y2
    """)
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('file', nargs='?')
    parser.add_argument('--delim', default='\t')
    parser.add_argument('--no-row-names', action='store_true')
    args = parser.parse_args()
    
    input = fileinput.FileInput([args.file] if args.file is not None else [])
    reader = csv.reader(input)
    column_names = input.next()
    row_names = []
    rows = []
    i = 1
    for r in input:
        if not args.no_row_names:
            row_names.push(r[0])
            rows.push(r[1:])
        else:
            row_names.push(i)
            rows.push(r)
            i += 1
    input.close()
    matrix_row_keys(column_names, row_names, rows)

def matrix_row_keys(column_names, row_names, rows):
    # X = collections.defaultdict(lambda: collections.defaultdict(set))
    X = dict((x, {}) for x in column_names)
    for y, row in izip(row_names, rows):
        for x, v in izip(column_names, row): 
            if v not in X[x]:
                X[x][v] = set()
            X[x][v].add(y)

    def intrsct(keys, X_y, key, remaining, R):
        if len(R) == 1:
            keys.add(frozenset(key))
        elif len(R) == 0 or len(remaining) == 0:
            pass
        else:
            # intersect R with the remaining sets containing y
            I = [(i, R.intersection(X_y[i][1])) for i in remaining]
            m = min(len(intr) for (i, intr) in I)
            for i, intr in I:
                if len(intr) == m:
                    k = key.copy()
                    rem = remaining.copy()
                    k.add(i)
                    rem.remove(i)
                    intrsct(keys, X_y, k, rem, intr)
            return keys

    # y -> { { (x, v) } }
    # K = collections.defaultdict(set)
    K = {}
    for y, row in izip(row_names, rows):
        X_y = [ifilter(lambda (v, ys): y in ys, X[x].iteritems()).next() for x in column_names]
        keys = set()
        m = min(len(R) for (v, R) in X_y)
        for i, (v, R) in enumerate(X_y):
            if len(R) == m:
                remaining = set(xrange(0, len(column_names)))
                # find the smallest set containing row_name
                key = set([i])
                remaining.remove(i)
                intrsct(keys, X_y, key, remaining, R)
        K[y] = set(
            frozenset((column_names[i], X_y[i][0]) for i in k) for k in keys
        )

    return K

if __name__ == '__main__':
    main()
