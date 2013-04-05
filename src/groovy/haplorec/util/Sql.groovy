package haplorec.util

class Sql {
	private static def DEFAULT_ENGINE_SAVE_AS = 'MyISAM'
    
	/*
	 * Keyword Arguments:
	 * 
	 * required:
	 * 
	 * one of:
	 *   String query: query on columns of existing table (query is executed with limit 0)
	 *   [String] columns and String existingTable: columns to use from existing table
	 * 
	 * optional:
	 * 
	 *   [String] indexColumns: columns on which to create an index (default: don't add an index)
	 *   boolean dontRunQuery: append 'limit 0' to the query (see query keyword argument) so that we don't 
	 *                         actually run the query; this is useful if we just want the columns and their 
	 *                         associated data types from a query string for our new table (default: false)
	 *   
	 */
	static def createTableFromExisting(Map kwargs = [:], groovy.sql.Sql sql, newTable, saveAs = DEFAULT_ENGINE_SAVE_AS) {
		if (!engines.any { saveAs == it }) {
			throw new IllegalArgumentException("saveAs must be a valid MySQL engine type")
		}
		if (kwargs.dontRunQuery == null) { kwargs.dontRunQuery = false }
		if (kwargs.query == null && !(kwargs.columns != null && kwargs.existingTable != null)) {
			throw new IllegalArgumentException("must provide one of 'query' or ('columns' and 'existingTable') in keyword arguments")
		}
		def q = (kwargs.query != null) ?
			kwargs.query :
			"select ${kwargs.columns.join(', ')} from ${kwargs.existingTable}".toString()
		// create the temporary table using the right datatypes
		sql.executeUpdate "create temporary table $newTable as ($q) ${(kwargs.dontRunQuery) ? 'limit 0' : ''}".toString()
		// create the temporary table using the right datatypes
		sql.executeUpdate "alter table $newTable engine = $saveAs".toString()
		if (kwargs.indexColumns != null) {
			def createIndex = { cols -> sql.executeUpdate "alter table $newTable add index (${cols.join(', ')})".toString() }
			if (kwargs.indexColumns[0] instanceof java.util.List) {
				kwargs.indexColumns.each { cols -> createIndex(cols) }
			} else {
				createIndex(kwargs.indexColumns)
			}
		}
	}
	
    // saveAs = (MEMORY|MyISAM|query|existing)
    static def selectWhereSetContains(Map kwargs = [:], groovy.sql.Sql sql, singlesetTable, multisetTable, setColumns, multisetGroupColumns, intoTable, saveAs = DEFAULT_ENGINE_SAVE_AS) {
        def setColumnsStr = setColumns.join(', ')
        def multisetGroupColumnsStr = multisetGroupColumns.join(', ')
        def query = { into = '' ->
            return """\
			select distinct ${multisetGroupColumns.collect { "counts_table.$it" }.join(', ')} from ${multisetTable} outer_table
			$into
			join (
			    select ${multisetGroupColumnsStr}, count(*) as group_count
			    from ${multisetTable} join ${singlesetTable} using (${setColumnsStr})
			    group by ${multisetGroupColumnsStr} 
			) counts_table
			where 
				${multisetGroupColumns.collect { "counts_table.$it = outer_table.$it" }.join(' and ')} 
				and counts_table.group_count = least(
				    (select count(*) from ${singlesetTable}), 
				    (select count(*) from ${multisetTable} inner_table where ${multisetGroupColumns.collect { "inner_table.$it = outer_table.$it" }.join(' and ')})
				)
            """
        }
        return selectAs(sql, query, multisetGroupColumns, saveAs, intoTable, indexColumns:kwargs.indexColumns)
    }

	// columnMap = ['x':'x', 'y':['y1', 'y2']]
	// A(x, y) B(x, y1, y2)
	//   1  2    1  2   3
	//   1  3
	static def groupedRowsToColumns(Map kwargs = [:], groovy.sql.Sql sql, rowTable, columnTable, groupBy, columnMap) {
		//defaults
		def badGroup = (kwargs.badGroup == null) ? { r -> } : kwargs.badGroup
		// sqlInsert == null
		// orderRowsBy == null
		if (groupBy instanceof java.lang.CharSequence) {
			groupBy = [groupBy]
		}
		def orderBy = (kwargs.orderRowsBy != null) ? groupBy + kwargs.orderRowsBy : groupBy
		def maxGroupSize = columnMap.values().grep { it instanceof java.util.List }.collect { it.size() }.max()
		if (maxGroupSize == null)  {
			maxGroupSize = 1
		}
		def columnTableColumns = columnMap.values().flatten()
		def columnTableColumnStr = columnTableColumns.join(', ')
		def insertGroup = { sqlI, g ->
			sqlI.withBatch("insert into ${columnTable}(${columnTableColumnStr}) values (${(['?'] * columnTableColumns.size()).join(', ')})".toString()) { ps ->
				if (g.size() > maxGroupSize) {
					badGroup(g)
				} else {
					def i = 0
					def values = columnTableColumns.inject([:]) { m, k -> m[k] = null; m }
					g.each { row ->
						row.keySet().each { k ->
							if (columnMap[k] instanceof java.util.List && i < columnMap[k].size()) {
								values[columnMap[k][i]] = row[k]
							} else if (i == 0 && !(columnMap[k] instanceof java.util.List)) {
								values[columnMap[k]] = row[k]
							}
						}
						i += 1
					}
					ps.addBatch(columnTableColumns.collect { values[it] })
				}
			}
		}
		def lastRowGroup = null
		List groups = []
		List group = []
		def rowCols = columnMap.keySet()
		sql.eachRow("select * from ${rowTable} order by ${orderBy.join(', ')}".toString()) { row ->
			def nextRowGroup = groupBy.collect { row[it] }
			if (lastRowGroup == null) {
				lastRowGroup = nextRowGroup
				group.add(rowCols.inject([:]) { m, c -> m[c] = row[c]; m })
			} else if (lastRowGroup == nextRowGroup) {
				group.add(rowCols.inject([:]) { m, c -> m[c] = row[c]; m })
			} else {
				// process a group
				if (sqlInsert == null) {
					groups.add(group)
				} else {
					insertGroup(sqlInsert, group)
					group = []
				}
			}
		}
		if (group.size() != 0) {
			groups.add(group)
		}
		groups.each { g ->
			insertGroup(sql, g)
		}
	}
	
	// TODO: add parameter for adding indexColumns instead of just depending on indexing columns all the time
    private static def engines = ['MEMORY', 'MyISAM']
    private static def validSaveAs = engines + ['query', 'existing']
    private static def selectAs(Map kwargs = [:], groovy.sql.Sql sql, Closure query, columns, saveAs = 'query', intoTable = null) {
        if (engines.any { saveAs == it }) {
            def q = query()
			createTableFromExisting(sql, intoTable, saveAs, query:q, indexColumns:kwargs.indexColumns)
			// TODO: figure out why i decided not run the query in the create table prior...
//            def qInsertInto = query('INTO $intoTable')
//            sql.executeUpdate qInsertInto
        } else if (saveAs == 'query') {
            return query()
        } else if (saveAs == 'existing') {
            def qInsertInto = query('INTO $intoTable')
            sql.executeUpdate qInsertInto
        } else {
            throw new IllegalArgumentException("Unknown saveAs type for outputting SQL results; saveAs was $saveAs but must be one of " + validSaveAs.join(', '))
        }
    }

}
