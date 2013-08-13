"""
Use funcparserlib to process string data from PharmGKB into uniform formats (e.g. remove trailing 
periods, lower-casing everything).
"""

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

def phenotype_name(phenotype_name):
    """
    Process the "Phenotype" part of a "Look up your guideline" drug recommendation.

    e.g. phenotype_name's for CYP2D6

    >>> phenotype_name('Intermediate metabolizer (~2-11% of patients)')
    'intermediate metabolizer'

    >>> phenotype_name('Intermediate Metabolizer (~2-11% of patients)')
    'intermediate metabolizer'
    """
    if phenotype_name is None:
        return None
    return ' '.join(_without_percent_of_patients.parse(_tokenize(phenotype_name.rstrip('.').lower())))
