package haplorec.util

import java.util.Map;

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
	 *   boolean temporary: make a temporary table (default: false)
	 *   
	 */
	static def createTableFromExisting(Map kwargs = [:], groovy.sql.Sql sql, newTable) {
        setDefaultKwargs(kwargs)
		if (!engines.any { kwargs.saveAs == it }) {
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
        def createTableStmt = "create ${(kwargs.temporary) ? 'temporary' : ''} table $newTable as ($q) ${(kwargs.dontRunQuery) ? 'limit 0' : ''}".toString()
        if (kwargs.sqlParams != null) {
            sql.executeUpdate createTableStmt, kwargs.sqlParams
        } else {
            sql.executeUpdate createTableStmt
        }
		// create the temporary table using the right datatypes
		sql.executeUpdate "alter table $newTable engine = ${kwargs.saveAs}".toString()
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
    static def selectWhereSetContains(Map kwargs = [:], groovy.sql.Sql sql, singlesetTable, multisetTable, setColumns, multisetGroupColumns, intoTable) {
        setDefaultKwargs(kwargs)
        if (kwargs.sqlParams == null) { kwargs.sqlParams = [:] }
		// use as List to ensure a consistent iteration ordering for proper query construction
		def sqlParamsColumns = kwargs.sqlParams.keySet() as List
        def setColumnsStr = setColumns.join(', ')
        def multisetGroupColumnsStr = multisetGroupColumns.join(', ')
		// WARNING: singlesetWhere might conflict with multisetTable columns (workaround would be to prefix singleset columns with table name)
        def query = """\
			select distinct ${(sqlParamsColumns.collect { ":$it" } + multisetGroupColumns.collect { "counts_table.$it" }).join(', ')} from ${multisetTable} outer_table
			join (
			    select ${multisetGroupColumnsStr}, count(*) as group_count
			    from ${multisetTable} join ${singlesetTable} using (${setColumnsStr})
				${(kwargs.singlesetWhere != null) ? "where ${kwargs.singlesetWhere}" : ''}
			    group by ${multisetGroupColumnsStr} 
			) counts_table
			where 
				${multisetGroupColumns.collect { "counts_table.$it = outer_table.$it" }.join(' and ')} 
				and counts_table.group_count = least(
				    (select count(*) from ${singlesetTable} ${(kwargs.singlesetWhere != null) ? "where ${kwargs.singlesetWhere}" : ''}), 
				    (select count(*) from ${multisetTable} inner_table where ${multisetGroupColumns.collect { "inner_table.$it = outer_table.$it" }.join(' and ')})
				)
        """
        return selectAs(sql, query, sqlParamsColumns + multisetGroupColumns, 
			intoTable:intoTable, 
			indexColumns:kwargs.indexColumns, 
			saveAs:kwargs.saveAs,
            sqlParams:(kwargs.sqlParams == [:]) ? null : kwargs.sqlParams)
    }
	
	// saveAs = (MEMORY|MyISAM|query|existing)
	static def selectWhereSetContains2(Map kwargs = [:], groovy.sql.Sql sql, tableA, tableB, setColumns) {
		setDefaultKwargs(kwargs)
		assert kwargs.tableAGroupBy != null || kwargs.tableBGroupBy != null
		if (kwargs.tableBGroupBy == null) {
			kwargs.tableBGroupBy = kwargs.tableAGroupBy
			def tmp = tableA
			tableA = tableB
			tableB = tmp
			tmp = kwargs.tableAWhere
			kwargs.tableAWhere = kwargs.tableBWhere
			kwargs.tableBWhere = tmp  
		}
		if (kwargs.select == null) {
			if (kwargs.tableAGroupBy != null && kwargs.tableBGroupBy != null) {
				kwargs.select = kwargs.tableAGroupBy + kwargs.tableBGroupBy
			} else if (kwargs.tableAGroupBy != null) {
				kwargs.select = kwargs.tableAGroupBy
			} else {
				kwargs.select = kwargs.tableBGroupBy
			}
		}
		def groupBy = (kwargs.tableAGroupBy ?: []) + (kwargs.tableBGroupBy ?: [])
		def groupByColumnsStr = groupBy.join(', ')
		def selectColumnStr = { alias -> kwargs.select.collect { (it.matches(/^\?|:.*$/)) ? it : "$alias.$it" }.join(', ') }
		def groupCountWhere
		def evalWhere = { where, alias -> 
			(where instanceof Closure) ? where(alias) : where
		}
		if (kwargs.tableAWhere == null && kwargs.tableBWhere == null) {
			groupCountWhere = null
		} else if (kwargs.tableAWhere != null && kwargs.tableBWhere == null) {
			groupCountWhere = evalWhere(kwargs.tableAWhere, tableA)
		} else if (kwargs.tableAWhere == null && kwargs.tableBWhere != null) {
			groupCountWhere = evalWhere(kwargs.tableBWhere, tableB)
		} else {
			groupCountWhere = "(${evalWhere(kwargs.tableAWhere, tableA)}) and (${evalWhere(kwargs.tableBWhere, tableB)})"
		}
		def tableCount = { table, where, groupByColumns ->
			def whereConjunctions =	((where != null) ? ["(${evalWhere(where, 'inner_table')})"] : []) + (groupByColumns ?: []).collect { "inner_table.$it = counts_table.$it" }
			return "select count(*) from ${table} inner_table ${(whereConjunctions.size() > 0) ? "where " + whereConjunctions.join(' and ') : ''}"
		}
		def query = """\
			select distinct ${selectColumnStr('counts_table')} from ${tableB} outer_table
			join (
			    select ${groupByColumnsStr}, count(*) as group_count
			    from ${tableB} join ${tableA} using (${setColumns.join(', ')})
				${(groupCountWhere != null) ? "where ${groupCountWhere}" : ''}
			    group by ${groupByColumnsStr} 
			) counts_table
			where 
				${kwargs.tableBGroupBy.collect { "counts_table.$it = outer_table.$it" }.join(' and ')} 
				and counts_table.group_count = least(
				    (${tableCount(tableA, kwargs.tableAWhere, kwargs.tableAGroupBy)}), 
				    (${tableCount(tableB, kwargs.tableBWhere, kwargs.tableBGroupBy)})
				)
        """
		return selectAs(sql, query, [],
			intoTable:kwargs.intoTable,
			indexColumns:kwargs.indexColumns,
			saveAs:kwargs.saveAs,
			sqlParams:kwargs.sqlParams)
	}

	// columnMap = ['x':'x', 'y':['y1', 'y2']]
	// A(x, y) B(x, y1, y2)
	//   1  2    1  2   3
	//   1  3
	static def groupedRowsToColumns(Map kwargs = [:], groovy.sql.Sql sql, rowTable, columnTable, groupBy, columnMap) {
        if (kwargs.sqlParams == null) { kwargs.sqlParams = [:] }
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
		def sqlParamsColumns = kwargs.sqlParams.keySet() as List
		def sqlParamsValues = sqlParamsColumns.collect { kwargs.sqlParams[it] }
		def insertGroup = { sqlI, g ->
			sqlI.withBatch("insert into ${columnTable}(${(sqlParamsColumns + columnTableColumns).join(', ')}) values (${(['?'] * (sqlParamsColumns.size() + columnTableColumns.size())).join(', ')})".toString()) { ps ->
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
					ps.addBatch(sqlParamsValues + columnTableColumns.collect { values[it] })
				}
			}
		}
		def lastRowGroup = null
		List groups = []
		List group = []
		def rowCols = columnMap.keySet()
		def rowQuery = "select * from ${rowTable} ${(kwargs.rowTableWhere != null) ? "where ${kwargs.rowTableWhere}" : ''} order by ${orderBy.join(', ')}".toString() 
		def handleRow = { row ->
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
		if (kwargs.sqlParams != [:]) {
			sql.eachRow(rowQuery, kwargs.sqlParams) { r -> handleRow(r) }
		} else {
			sql.eachRow(rowQuery) { r -> handleRow(r) }
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
    private static def selectAs(Map kwargs = [:], groovy.sql.Sql sql, query, columns) {
        setDefaultKwargs(kwargs)
        if (engines.any { kwargs.saveAs == it }) {
			createTableFromExisting(sql, kwargs.intoTable, 
				saveAs:kwargs.saveAs, 
				query:query, 
				indexColumns:kwargs.indexColumns,
				sqlParams:kwargs.sqlParams)
			// TODO: figure out why i decided not run the query in the create table prior...
//            def qInsertInto = query('INTO ${kwargs.intoTable}')
//            sql.executeUpdate qInsertInto
        } else if (kwargs.saveAs == 'query') {
            return query
        } else if (kwargs.saveAs == 'existing') {
			def qInsertInto = """\
				insert into ${kwargs.intoTable} (${columns.join(', ')})
				$query""".toString()
//            def qInsertInto = query("INTO ${kwargs.intoTable}").toString()
            if (kwargs.sqlParams != null) {
                sql.execute qInsertInto, kwargs.sqlParams
            } else {
                sql.execute qInsertInto
            }
        } else {
            throw new IllegalArgumentException("Unknown saveAs type for outputting SQL results; saveAs was ${kwargs.saveAs} but must be one of " + validSaveAs.join(', '))
        }
    }
	
	static def insert(groovy.sql.Sql sql, table, columns, rows) {
		if (rows != null && rows.size() > 0) {
			sql.withBatch("insert into ${table}${(columns.size() > 0) ? "(${columns.join(', ')})" : ''} values (${(['?'] * rows[0].size()).join(', ')})".toString()) { ps ->
				rows.each { r -> ps.addBatch(r) }
			}
		}
	}
	
	static def insert(groovy.sql.Sql sql, table, rows) {
		insert(sql, table, [], rows)
	}
	
	private static def setDefaultKwargs(Map kwargs) {
		def setDefault = { property, defaultValue ->
			if (kwargs[property] == null) {
				kwargs[property] = defaultValue
			}
		}
		setDefault('saveAs', DEFAULT_ENGINE_SAVE_AS)
	}

    /* groovy.sql.Sql.* methods don't handle an empty parameter map well; this is just a convenience wrapper that hands such 
     * a method no params if the params map is empty
     */
    static def sqlWithParams(sqlMethod, stmt, params) {
        if (params != [:]) {
            return sqlMethod(stmt)
        } else {
            return sqlMethod(stmt, params)
        }
    }
	
	/* Execute a block of code using a unique identifier generated from the autoincrement column when inserting an empty row into the provided table Insert an empty row into $table,
	 * Keyword arguments:
	 * 
	 * optional:
	 * String idColumn: name of the autoincrement column for the provided table
	 * Map<String, T> values: a map from column names to values (useful if the table your using doesn't specify default values)
	 */
	static def withUniqueId(Map kwargs = [:], groovy.sql.Sql sql, table, Closure doWithId) {
		if (kwargs.idColumn == null) { kwargs.idColumn = 'id' }
		if (kwargs.values == null) { kwargs.values = [:] }
		if (kwargs.deleteAfter == null) { kwargs.deleteAfter = true }
		// make sure this works with multiple columns
		def extraColumns = kwargs.values.keySet()
		def keys = sql.executeInsert """\
			insert into $table(${extraColumns.join(', ')}) 
			values(${(['?']*extraColumns.size()).join(', ')})""".toString(), 
			extraColumns.collect { c -> kwargs.values[c] } 
		def id = keys[0][0]
		try {
			doWithId(id)
		} finally {
            if (kwargs.deleteAfter) {
                sql.execute "delete from $table where ${kwargs.idColumn} = ?", id
            }
		}
	}

}
