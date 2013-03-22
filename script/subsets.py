#!/usr/bin/env python
import itertools
import argparse

def main():
    parser = argparse.ArgumentParser(description="print all subsets of the set { i | i is in 1..n }")
    parser.add_argument('n', type=int)
    args = parser.parse_args()
    for subset in subsets(args.n):
        print '{', ','.join(str(x) for x in subset), '}'

def subsets(n):
    for size in xrange(n+1):
        for subset in itertools.combinations(range(1, n+1), size):
            yield subset

if __name__ == '__main__':
    main()
