from funcparserlib.parser import some, a, many, skip, finished, maybe, with_forward_decls, NoParseError, _Ignored

import re

def _tokenize(string):
    return [x for x in re.split(r"(~|-|%|\(|\))|\s+", string) if x is not None and not re.match(r'^$', x)]

_number = some(lambda s: re.match(r'\d+', s))
_any = some(lambda x: True)

def _remove_skipped(tokens):
    # work around the quirk of funcparserlib where _Ignored things are only removed if they're 
    # followed by a '+'
    return [x for x in tokens if not isinstance(x, _Ignored)]

_without_percent_of_patients = many( 
    skip( 
        # (~2[-11]% [of patients])
        a('(') + a('~') + _number + maybe( a('-') + _number ) + a('%') + maybe( a('of') + a('patients') ) + a(')')
    ) | 
    _any 
) >> _remove_skipped

# process fields scraped from pharmgkb into the same format (e.g. remove trailing periods, 
# lower-casing everything) to make duplicate filtering of items easier.

# e.g. phenotype_name's for CYP2D6
# 
# Intermediate metabolizer (~2-11% of patients)
# Intermediate Metabolizer (~2-11% of patients)
def phenotype_name(phenotype_name):
    if phenotype_name is None:
        return None
    return ' '.join(_without_percent_of_patients.parse(_tokenize(phenotype_name.rstrip('.').lower())))
