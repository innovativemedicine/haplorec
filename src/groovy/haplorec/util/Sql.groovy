package haplorec.util

class Sql {
    
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
	static def createTableFromExisting(Map kwargs = [:], Sql sql, newTable, saveAs = DEFAULT_ENGINE_SAVE_AS) {
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
		sql.executeUpdate "create temporary table $newTable as ($q) ${(kwargs.dontRunQuery) ? 'limit 0' : ''}"
		// create the temporary table using the right datatypes
		sql.executeUpdate "alter table $newTable engine = $saveAs"
		if (kwargs.indexColumns != null) {
			def createIndex = { cols -> sql.executeUpdate "alter table $newTable add index (${cols.join(', ')})" }
			if (kwargs.indexColumns[0] instanceof java.util.List) {
				kwargs.indexColumns.each { cols -> createIndex(cols) }
			} else {
				createIndex(kwargs.indexColumns)
			}
		}
	}
	
    // saveAs = (MEMORY|MyISAM|query|existing)
    static def selectWhereSetContains(Map kwargs = [:], Sql sql, singlesetTable, multisetTable, setColumns, multisetGroupColumns, saveAs, intoTable) {
        def setColumnsStr = setColumns.join(', ')
        def multisetGroupColumnsStr = multisetGroupColumns.join(', ')
        def query = { into = '' ->
            // alias for outer multisetTable of query
            def outer = "outer"
            def whereEitherSetContainsStr = whereEitherSetContains(
                "select $setColumnsStr from $singlesetTable", 
                "select $setColumnsStr from $multisetTable where " +
					// TODO: it may be useful to have an index on (multisetGroupColumns)
                    multisetGroupColumns.collect { "$it = $outer.$it" }.join('and '),
                setColumns
            )
            return """\
            select distinct $multisetGroupColumnsStr from $multisetTable $outer
                $into
                where $whereEitherSetContainsStr 
            """
        }
        return selectAs(sql, query, multisetGroupColumns, saveAs, intoTable, indexColumns:kwargs.indexColumns)
    }

    private static def whereEitherSetContains(queryA, queryB, setColumns) {
        // refer to src/sql/mysql/subset_query.sql 
        def setColumnsStr = setColumns.join(', ')
        return """\
        ( 
            select count(*) from (
                select * from ($queryA) A join ($queryB) B using ($setColumnsStr)
            ) tmp ) = least(($queryA), ($queryB))
        )
        """
    }

	// TODO: add parameter for adding indexColumns instead of just depending on indexing columns all the time
    private static def engines = ['MEMORY', 'MyISAM']
    private static def validSaveAs = engines + ['query', 'existing']
    private static def selectAs(Map kwargs = [:], Sql sql, Closure query, columns, saveAs = 'query', intoTable = null) {
        if (engines.any { saveAs == it }) {
            def q = query()
			createTableFromExisting(sql, intoTable, saveAs, query:query, indexColumns:kwargs.indexColumns)
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
