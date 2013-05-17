#!/usr/bin/env python
import csv
import fileinput
import argparse
import sys
import itertools

def main():
    parser = argparse.ArgumentParser(description="Given a matrix of haplotype and snp alleles for a gene, output a gene_haplotype_variant input file. The input looks like:\n" +
            "Haplotype Name   rs4244285   rs3758580\n" +
            "*1               G           C\n" +
            "*1A              G           C",
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('files', nargs="+")
    parser.add_argument('--gene', '-g', required=True)
    parser.add_argument('--delim', '-d', default="\t")
    args = parser.parse_args()
    for filename in args.files:
        to_table(sys.stdout, filename, args.gene,
                delim=args.delim)

def to_table(stream, filename, gene_name, delim="\t"):
    writer = csv.writer(stream, delimiter=delim)
    reader = csv.reader(fileinput.input(filename), delimiter=delim)
    snp_ids = reader.next()[1:]
    for row in reader:
        haplotype_name = row[0]
        alleles = row[1:]
        for snp_id, allele in itertools.izip(snp_ids, alleles):
            writer.writerow([ gene_name, haplotype_name, snp_id, allele ])

if __name__ == '__main__':
    main()
