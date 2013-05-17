def table(t):
    rows = t.select('*/tr | tr')
    header = rows[0].select('th')
    yield header
    for row in rows[1:]:
        yield row.select('td') 
