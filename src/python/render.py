#!/usr/bin/env python
import argparse
import os
from os import environ, path
from jinja2 import Environment, FileSystemLoader

def main():
    parser = argparse.ArgumentParser(description="Render a template in the snpdb project")
    parser.add_argument('template_file')
    parser.add_argument('--out', required=False, default=None, help="Output file.  Default is the same as the input file with .jinja suffix removed, or .out added if no such suffix exists")
    args = parser.parse_args()

    if args.out is None:
        root, ext = path.splitext(args.template_file)
        args.out = root if ext == '.jinja' else args.template_file + '.out'

    env = Environment(loader = FileSystemLoader(['./', '/']))

    import re
    import itertools
    modules = {
            're': re,
            'itertools': itertools,
            }

    modules.update(environ)
    try:
        with open(args.out, 'w') as f:
            print >>f, env.get_template(args.template_file).render(modules)
    except:
        os.remove(args.out)
        raise

if __name__ == '__main__':
    main()
