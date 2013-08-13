"""
Define some common functions for extracting data from a website.
"""

def table(t):
    """
    Given the HtmlXPathSelector for a table, extract it's row-wise contents.
    """
    rows = t.select('*/tr | tr')
    header = rows[0].select('th')
    yield header
    for row in rows[1:]:
        yield row.select('td') 

