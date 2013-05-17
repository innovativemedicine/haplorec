#!/usr/bin/env python
import requests
import argparse
import multiprocessing
import fileinput
import csv
import sys

def main():
    parser = argparse.ArgumentParser(description="Given a list of snp_id's as input, check for their existence on http://www.pharmgkb.org/ (at http://www.pharmgkb.org/<snp_id>)")
    parser.add_argument('files', nargs="+")
    parser.add_argument('--parallel', '-n', help="max number of current HTTP requests to make at a time (default: # of cpu's)", default=multiprocessing.cpu_count(), type=int)
    parser.add_argument('--delim', '-d', default="\t")
    args = parser.parse_args()
    
    num_snps = None
    if len(args.files) != 0 and '-' not in args.files:
        num_snps = sum(file_len(f) for f in args.files)

    def lines():
        for line in fileinput.input(args.files):
            yield line.rstrip()

    def print_snp(result):
        if args.missing and args.present:
            # write a prefix w
            sys.stdout.write("missing: ")

    writer = csv.DictWriter(sys.stdout, fieldnames=['found', 'snp_id', 'url'], delimiter=args.delim)
    writer.writeheader()
    for result in pharmgkb_snps([x for x in lines()], 
                                n=args.parallel,
                                chunksize=max(num_snps/args.parallel, 1) if num_snps is not None else 1):
        writer.writerow(result)

def pharmgkb_snp(snp):
    url = "http://www.pharmgkb.org/rsid/{snp}".format(**locals())
    r = requests.get(url)
    if r.status_code in [200, 404]:
        return {'found': r.status_code == 200, 'snp_id': snp, 'url': url}
    else:
        raise RuntimeError("Expected a 200 / 404 status code but saw {status} when getting {url}".format(url=url, status=r.status_code))

def pharmgkb_snps(snps, n=multiprocessing.cpu_count(), chunksize=1):
    pool = multiprocessing.Pool(n)
    return pool.imap_unordered(pharmgkb_snp, snps, chunksize)

def file_len(fname):
    i = 0
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

if __name__ == '__main__':
    main()
