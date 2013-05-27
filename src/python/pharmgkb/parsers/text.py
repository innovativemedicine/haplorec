from funcparserlib.parser import some, a, many, skip, finished, maybe, with_forward_decls, NoParseError
from tokenize import generate_tokens 
import token
from StringIO import StringIO
import re

# Implement a simple tokenizer
# Reference: http://deplinenoise.wordpress.com/2012/01/04/python-tip-regex-based-tokenizer/

class Token(object):
    def __init__(self, type, value, line=''):
        self.type = type
        self.value = value
        self.line = line

    def __repr__(self):
        return 'Token(%r, %r)' % (self.type, self.value)

    def __eq__(self, other):
        return (self.type, self.value) == (other.type, other.value)

def scanner(tokens):
    matcher = re.compile('|'.join(r"({regex})".format(regex=t[0]) for t in tokens))
    def scan(s):
        for match in re.finditer(matcher, s):
            groups = match.groups()
            i = 0
            for g in groups:
                if g is not None:
                    break
                i += 1
            yield Token(tokens[i][1], g, s)
    return scan

_tokens = [
    [ r'\d+', 'NUMBER' ],
    [ r'[^\s]+', 'STRING' ], 
]
_scanner = scanner(_tokens)
def tokens(s):
    return _scanner(s)

# Parse the tokens into a useful form
# Reference: https://bitbucket.org/vlasovskikh/funcparserlib/src/0.3.6/doc/Tutorial.md?at=0.3.x

def pattern(*patterns, **kwargs):
    if 'flags' not in kwargs:
        kwargs['flags'] = 0
    def pat_parser(p):
        def match(t):
            return re.search(p, t.value, kwargs['flags'])
        return some(match)
    pat = pat_parser(patterns[0])
    for p in patterns[1:]:
        pat = pat + pat_parser(p)
    return pat

def ipattern(*patterns, **kwargs):
    if 'flags' not in kwargs:
        kwargs['flags'] = 0
    return pattern(*patterns, flags= kwargs['flags'] | re.IGNORECASE)

def tokval(t):
    if t.__class__ == Token:
        return t.value
    else:
        return [token.value for token in t]

def _join_vals(tl):
    return ' '.join(tokval(tl))
def _allele(allele_str, count_prefix):
    return (
        skip(count_prefix) + 
        ( (many( ipattern(r'^(?!{}).*'.format(allele_str)) )) >> _join_vals ) + 
        skip( ipattern(r'{}'.format(allele_str)) )
    )
# E.g.
# "one functional allele and one gain-of-function allele"
_one_allele = _allele('allele', ipattern(r'one'))
# E.g.
# "two gain-of-function alleles"
_two_alleles = _allele('alleles', ipattern(r'two|only') | ( ipattern('duplications') + ipattern('of')))

def separated(parser, by):
    def append(parsed):
        first = parsed[0]
        rest = parsed[1]
        rest.insert(0, first)
        return rest
    return ( parser + many(by + parser) ) >> append

# E.g. 
# "An individual carrying two gain-of-function alleles or one functional allele and one gain-of-function allele"
# => [('gain-of-function', 'gain-of-function'), ('functional', 'gain-of-function')]
phenotype_genotype = ( 
    skip( ipattern(r'an', r'individual', r'carrying') ) + 
    separated(
       ( _two_alleles >> (lambda allele: ( allele, allele )) ) | 
       ( _one_allele + skip(ipattern(r'and')) + _one_allele ),
       skip(maybe(ipattern(r'or')))
    )
)

_parsers = {
    'phenotype_genotype': phenotype_genotype,
}
class ParserError(Exception):
    pass
def parse(parser_name, string):
    parser = _parsers[parser_name]
    toks = [x for x in tokens(string)]
    try:
        return parser.parse(toks)
    except NoParseError as e:
        raise ParserError("Failed to parse {thing_to_parse}: \"{string}\" at {token}".format(
            string=string, token=toks[e.state.max-1], thing_to_parse=parser_name,
        ))
