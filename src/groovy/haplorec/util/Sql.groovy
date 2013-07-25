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
        if (kwargs.onDuplicateKey == 'discard' && kwargs.columns == null) {
            throw new IllegalArgumentException("must provide a columns argument when onDuplicateKey == 'discard'")
        }
		if (!engines.any { kwargs.saveAs == it }) {
			throw new IllegalArgumentException("saveAs must be a valid MySQL engine type")
		}
		if (kwargs.dontRunQuery == null) { kwargs.dontRunQuery = false }
		if (kwargs.query == null && !(kwargs.columns != null && kwargs.existingTable != null)) {
			throw new IllegalArgumentException("must provide one of 'query' or ('columns' and 'existingTable') in keyword arguments")
		}
		def query = (kwargs.query != null) ?
			kwargs.query :
			"select ${kwargs.columns.join(', ')} from ${kwargs.existingTable}".toString()
        def createTablePrefix = "create ${(kwargs.temporary) ? 'temporary' : ''} table $newTable engine=${kwargs.saveAs}"
        if (kwargs.dontRunQuery) {
            // create the temporary table using the right datatypes
            _sql kwargs, sql.&executeUpdate, "$createTablePrefix as ($query) limit 0"
        } else {
            // insert our query
            // def qInsertInto = insertIntoSql(kwargs, newTable, query)
            // _sql kwargs, sql.&executeUpdate, qInsertInto
            _sql kwargs, sql.&executeUpdate, "$createTablePrefix as ($query)"
        }
		if (kwargs.indexColumns != null) {
			def createIndex = { cols -> sql.executeUpdate "alter table $newTable add index (${cols.join(', ')})".toString() }
			if (kwargs.indexColumns[0] instanceof java.util.List) {
				kwargs.indexColumns.each { cols -> createIndex(cols) }
			} else {
				createIndex(kwargs.indexColumns)
			}
		}
	}

    /* Keyword arguments:
     *
     * intersectTable: the table in which to store the result of 'intersecting' tables A and B. Providing this will slow down 
     * the intersect query, but it could be useful if you wanted to reuse the results of the intersect table for multiple 
     * selectWhereSetContains queries.  The schema of the table should be tableAGroupBy + tableBGroupBy + ['group_count']. 
     */
	private static def intersectQuery(Map kwargs = [:], groovy.sql.Sql sql, tableA, tableB, setColumns, Closure countsTableWhere) {
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
        def countsTableWhereStr = countsTableWhere(
            "counts_table.group_count", 
            tableCount(tableA, kwargs.tableAWhere, kwargs.tableAGroupBy), 
            tableCount(tableB, kwargs.tableBWhere, kwargs.tableBGroupBy))

        def intersectQuery = """\
            |select ${groupByColumnsStr}, count(*) as group_count
            |from ${tableB} join ${tableA} using (${setColumns.join(', ')})
            |${(groupCountWhere != null) ? "where ${groupCountWhere}" : ''}
            |group by ${groupByColumnsStr} 
        """.stripMargin()
        def queryWithCountsTable = { countsTable -> """\
            |select distinct ${selectColumnStr('counts_table')} from $countsTable counts_table
            |where 
            |    ${countsTableWhereStr}""".stripMargin()
        }
        def query
        if (kwargs.intersectTable == null) {
            // counts table is a derived table
            query = queryWithCountsTable("""\
                |(
                |    $intersectQuery
                |)
            """.stripMargin())
        } else {
            // counts table is an existing table kwargs.intersectTable (probably with indexes on it to make it have faster access during group_count filtering)
            selectAs(sql, intersectQuery, groupBy + ['group_count'],
                intoTable:kwargs.intersectTable,
                saveAs:'existing',
                sqlParams:kwargs.sqlParams)
            query = queryWithCountsTable(kwargs.intersectTable)
        }
		return selectAs(sql, query, kwargs.select,
			intoTable:kwargs.intoTable,
			indexColumns:kwargs.indexColumns,
			saveAs:kwargs.saveAs,
			onDuplicateKey:kwargs.onDuplicateKey,
			sqlParams:kwargs.sqlParams)

    }

    // A contains B or B contains A
	static def selectWhereEitherSetContains(Map kwargs = [:], groovy.sql.Sql sql, tableA, tableB, setColumns) {
        return intersectQuery(kwargs, sql, tableA, tableB, setColumns) { intersectSize, ASizeQuery, BSizeQuery ->
            return """\
                |$intersectSize = least(
                |    ( $ASizeQuery ),
                |    ( $BSizeQuery )
                |)
            """.stripMargin()
        }
	}

    // A contains B
	static def selectWhereSetContains(Map kwargs = [:], groovy.sql.Sql sql, tableA, tableB, setColumns) {
        return intersectQuery(kwargs, sql, tableA, tableB, setColumns) { intersectSize, ASizeQuery, BSizeQuery ->
            return """\
                |$intersectSize >= (
                |    $ASizeQuery
                |)
            """.stripMargin()
        }
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
		def insertGroup = { sqlI, g ->
			sqlI.withBatch("insert into ${columnTable}(${columnTableColumns.join(', ')}) values (${(['?'] * columnTableColumns.size()).join(', ')})".toString()) { ps ->
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
		def rowQuery = "select * from ${rowTable} ${(kwargs.rowTableWhere != null) ? "where ${kwargs.rowTableWhere}" : ''} order by ${orderBy.join(', ')}".toString() 
		def handleRow = { row ->
			def nextRowGroup = groupBy.collect { row[it] }
			def addRowToGroup = { -> group.add(rowCols.inject([:]) { m, c -> m[c] = row[c]; m }) }
			if (lastRowGroup == null) {
				addRowToGroup()
			} else if (lastRowGroup == nextRowGroup) {
				addRowToGroup()
			} else {
				// process the last group
				if (kwargs.sqlInsert == null) {
					groups.add(group)
				} else {
					insertGroup(kwargs.sqlInsert, group)
				}
				// start a new group
				group = []
				addRowToGroup()
			}
			lastRowGroup = nextRowGroup
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

    private static def _sql(Map kwargs = [:], sqlMethod, String stmt) {
        if (kwargs.sqlParams != null) {
            sqlMethod stmt, kwargs.sqlParams
        } else {
            sqlMethod stmt
        }
    }

    private static def _(Map kwargs = [:], value) {
        if (kwargs.default == null) { kwargs.default = '' }
        if (!kwargs.containsKey('null')) { kwargs.null = null }
        if (kwargs.default == null) { kwargs.default = '' }
        if (kwargs.return == null) { kwargs.return = { x -> x } }
        if (kwargs.ret == null) { 
            kwargs.ret = { x ->
                if (x != kwargs.null) {
                    kwargs.return(x)
                } else {
                    kwargs.default
                }
            }
        }
        kwargs.ret(value)
    }
	
	private static String insertIntoSql(Map kwargs = [:], intoTable, query) {
		if (kwargs.onDuplicateKey == null) {
            return """\
               |insert into $intoTable (${_(kwargs.columns, return: { it.join(', ') })}) 
               |$query""".stripMargin()
        } else {
            def update
            if (kwargs.onDuplicateKey == 'discard') { 
                // This ignores the insert, keeping an existing row.
                // http://stackoverflow.com/questions/2366813/on-duplicate-key-ignore
                update = "$intoTable.${kwargs.columns[0]} = $intoTable.${kwargs.columns[0]}" 
            } else {
                assert kwargs.onDuplicateKey instanceof Closure : "onDuplicateKey argument is a closure taking old and new table aliases for $intoTable"
                update = kwargs.onDuplicateKey('old', intoTable)
            }
            return """\
               |insert into $intoTable (${_(kwargs.columns, return: { it.join(', ') })}) 
               |    (select * from ($query) old)
               |on duplicate key update $update""".stripMargin()
		}
	}
	
    private static def engines = ['MEMORY', 'MyISAM', 'InnoDB']
    private static def validSaveAs = engines + ['query', 'existing', 'rows', 'iterator']
    private static def selectAs(Map kwargs = [:], groovy.sql.Sql sql, query, columns) {
        setDefaultKwargs(kwargs)
        if (engines.any { kwargs.saveAs == it }) {
			createTableFromExisting(sql, kwargs.intoTable, 
				saveAs:kwargs.saveAs, 
				query:query, 
				indexColumns:kwargs.indexColumns,
                onDuplicateKey:kwargs.onDuplicateKey,
				sqlParams:kwargs.sqlParams)
        } else if (kwargs.saveAs == 'query') {
            return query
        } else if (kwargs.saveAs == 'existing') {
            def qInsertInto = insertIntoSql(kwargs + [columns:columns], kwargs.intoTable, query)
            _sql kwargs, sql.&execute, qInsertInto
        } else if (kwargs.saveAs == 'rows') {
            _sql kwargs, sql.&rows, query
        } else if (kwargs.saveAs == 'iterator') {
            return new Object() {
                def each(Closure f) {
                    if (kwargs.sqlParams != null) {
                        sql.eachRow(query, kwargs.sqlParams, f)
                    } else {
                        sql.eachRow(query, f)
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown saveAs type for outputting SQL results; saveAs was ${kwargs.saveAs} but must be one of " + validSaveAs.join(', '))
        }
    }
	
	static def insert(groovy.sql.Sql sql, table, columns, rows) {
        if (rows == null) {
            return
        }
        int rowSize
        if (rows instanceof List) {
            if (rows.size() == 0) {
                return
            }
            rowSize = rows[0].size()
            if (columns == null && rows[0] instanceof LinkedHashMap) {
                columns = rows[0].keySet()
            }
        } else {
            rowSize = columns.size()
        }

        def qmarks = { n -> (['?'] * n).join(', ') }
        if (rowSize != 0) {
            File infile = File.createTempFile("${table}_table", '.tsv')
            try {
                infile.withPrintWriter() { w ->
                    rows.each { row ->
                        // TOOD: handle quoting
                        w.println(
                            (row instanceof LinkedHashMap ? row.values() : row).collect { column ->
                                if (column == null) {
                                    // We need to use \N in the data file to represent a NULL value in mysql
                                    // http://stackoverflow.com/questions/2675323/mysql-load-null-values-from-csv-data
                                    '\\N'
                                } else {
                                    column
                                }
                            }.join('\t')
                        )
                    }
                }
                sql.execute "load data local infile :infile into table ${table}${(columns.size() > 0) ? "(${columns.join(', ')})" : ''}", [infile: infile.absolutePath]
            } finally {
                infile.delete()
            } 
            // NOTE: this batch insert thing is slow.
            // String stmt = "insert into ${table}${(columns.size() > 0) ? "(${columns.join(', ')})" : ''} values (${qmarks(rowSize)})"
            // 
            // sql.withBatch(stmt) { ps ->
            //     rows.each { r -> 
            //         ps.addBatch(r) 
            //     }
            // }
        } else {
            rows.each { r ->
                sql.execute("insert into ${table}() values (${qmarks(r.size())})", r)
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
	
	private static def hashRowsToListRows(rows, cols) {
		rows.collect { r ->
			cols.collect { r[it] }
		}
	}

    private static def columnMetadata(Map kwargs = [:], groovy.sql.Sql sql) {
        if (kwargs.select == null) { kwargs.select = ['column_name'] }
		_sql(kwargs, sql.&rows, """\
			|select ${ kwargs.select.join(', ') }
			|from information_schema.columns 
			|where table_schema = database()
            |""".stripMargin())
    }

    static def tblColumns(Map kwargs = [:], groovy.sql.Sql sql) {
        def columns = columnMetadata(sql,
            select: ['table_name', 'column_name', 'column_key'],
        )
        /* tables: [
         *   table1: [
         *      columns: [col1, col2, ...],
         *      primaryKey: [col1, col2],
         *   ]
         * ]
         */
        def tables = [:]
        columns.each { r ->
            if (!tables.containsKey(r.table_name)) {
                tables[r.table_name] = [
                    columns: [],
                    primaryKey: [],
                ]
            }
            tables[r.table_name].columns.add(r.column_name) 
            if (r.column_key == 'PRI') {
                tables[r.table_name].primaryKey.add(r.column_name) 
            }
        }
        return tables
    }
	
	static def tableColumns(Map kwargs = [:], groovy.sql.Sql sql, table) {
        kwargs.sqlParams = (kwargs.sqlParams ?: [:]) + [table: table]
		hashRowsToListRows(_sql(kwargs, sql.&rows, """\
			|select column_name 
			|from information_schema.columns 
			|where table_schema = database() and
			|      table_name = :table
			|	   ${_(kwargs.where, return: { "and ( $it )" })}""".stripMargin()),
			['column_name']).collect { it[0] }
	}

    /* Wrapper for Sql.eachRow that replaces columns at positions 0..n with kwargs.names 0..n
    */
    static def rows(Map kwargs = [:], groovy.sql.Sql sql, query) {
        new Object() {
            def each(Closure f) {

                def eachRow
                if (kwargs.names != null) {
                    eachRow = { row ->
                        Map mapRow = [:]
                        (0..row.getMetaData().getColumnCount()-1).each { i ->
                            mapRow[kwargs.names[i]] = row[i]
                        }
                        f(mapRow)
                    }
                } else {
                    eachRow = f
                }

                if (kwargs.sqlParams != null && kwargs.sqlParams != [:]) {
                    sql.eachRow(query, kwargs.sqlParams, eachRow) 
                } else {
                    sql.eachRow(query, eachRow)
                }
            }
        }    
    }

}
