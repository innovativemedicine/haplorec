package haplorec.util

import java.util.Map;

class Sql {
	private static def DEFAULT_ENGINE_SAVE_AS = 'MyISAM'

	/** Create a new table from a query on an existing table.
	 * 
	 * Required:
	 * @param kwargs.query
     * a string query on columns of existing table (query is executed with limit 0)
     * Or:
	 * @param kwargs.columns 
     * columns to query
     * @param kwargs.existingTable
     * existing table to query from
	 * 
	 * Optional:
     * @param kwargs.indexColumns
     * list of columns on which to create an index (or a list of lists of columns for multiple 
     * indexes) (default: don't add an index)
     * @param kwargs.dontRunQuery
     * when true, append 'limit 0' to the query (see query keyword argument) so that we don't 
     * actually run the query; this is useful if we just want the columns and their associated data 
     * types from a query string for our new table (default: false)
	 * @param kwargs.temporary
     * when true, make the new table temporary (default: false)
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
		def query = (kwargs.query != null) ?
			kwargs.query :
			"select ${kwargs.columns.join(', ')} from ${kwargs.existingTable}".toString()
        def createTablePrefix = "create ${(kwargs.temporary) ? 'temporary' : ''} table $newTable engine=${kwargs.saveAs}"
        if (kwargs.dontRunQuery) {
            /* Create the table using the right datatypes.
             */
            _sql kwargs, sql.&executeUpdate, "$createTablePrefix as ($query) limit 0"
        } else {
            /* Insert our query.
             */
            _sql kwargs, sql.&executeUpdate, "$createTablePrefix as ($query)"
        }
		if (kwargs.indexColumns != null) {
			def createIndex = { cols -> sql.executeUpdate "alter table $newTable add index (${cols.join(', ')})".toString() }
			if (kwargs.indexColumns[0] instanceof java.util.List) {
                /* Create multiple indexes.
                 */
				kwargs.indexColumns.each { cols -> createIndex(cols) }
			} else {
                /* Create a single index.
                 */
				createIndex(kwargs.indexColumns)
			}
		}
	}

    /** Model a set relation operation between two tables A and B by joining them on their "set 
     * columns", then grouping by each table's groupByColumns (rows with the same groupByColumns 
     * constitute a single set), comparing the count in each intersected group to the size of the 
     * original groups (sets) from tables A and B to determine which rows to keep (depending on what 
     * set relation you want to model).
     *
     * See selectWhereSubsetOf for a concrete example of a set relation being modelled.
     *
     * @param setColumns
     * the names of columns from both table A and table B that are used to represent fields in a 
     * set.
     * For example, if we have two tables:
     * A(a, b, x, y) // represents: a, b, { (x, y) }
     * B(z, x, y)    // represents: z, { (x, y) }
     * setColumns = ['x', 'y']
     * @param kwargs.tableAGroupBy
     * the columns in table A that identify rows belonging to the same set.
     * For example:
     * kwargs.tableAGroupBy = ['a', 'b'] for the example in setColumns
     * @param kwargs.tableBGroupBy
     * same idea as kwargs.tableAGroupBy but for B.
     * For example:
     * kwargs.tableAGroupBy = ['z'] for the example in setColumns
     * @param countsTableWhere
     * a function of type ( intersect_count_field, table_A_set_size, table_B_set_size -> whereCondition )
     * that determines which rows to keep based on the set relation we want to model (see 
     * selectWhereSubsetOf as an example).
     * @param kwargs.tableAWhere
     * restrict the operation on table A to rows matching this where clause
     * @param kwargs.tableAWhere
     * same idea as kwargs.tableAWhere but for B
     * @param kwargs.intersectTable
     * the table in which to store the result of 'intersecting' tables A and B. Providing this will 
     * slow down the intersect query, but it could be useful if you wanted to reuse the results of 
     * the intersect table for multiple selectWhereSubsetOf queries.  The schema of the table 
     * should be tableAGroupBy + tableBGroupBy + ['group_count']. 
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
            /* Counts table is a derived table.
             */
            query = queryWithCountsTable("""\
                |(
                |    $intersectQuery
                |)
            """.stripMargin())
        } else {
            /* Counts table is an existing table kwargs.intersectTable (probably with indexes on it to make it have faster access during group_count filtering).
             */
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

    /** A is a subset of B or B is a subset of A.
     *
     * For example, if we have two tables:
     * A(a, b, x, y) // represents: a, b, { (x, y) }
     * B(z, x, y)    // represents: z, { (x, y) }
     *
     * If we want to model finding sets a in A that are a subet of b in B, or where b is a subset of 
     * a, then we know |a intersect b| = min(|a|, |b|) is sufficient. 
     *
     * Same parameters as intersectQuery, minus countsTableWhere.
     */
	static def selectWhereEitherSubsetOf(Map kwargs = [:], groovy.sql.Sql sql, tableA, tableB, setColumns) {
        return intersectQuery(kwargs, sql, tableA, tableB, setColumns) { intersectSize, ASizeQuery, BSizeQuery ->
            return """\
                |$intersectSize = least(
                |    ( $ASizeQuery ),
                |    ( $BSizeQuery )
                |)
            """.stripMargin()
        }
	}

    /** A is a subset of B.
     *
     * For example, if we have two tables:
     * A(a, b, x, y) // represents: a, b, { (x, y) }
     * B(z, x, y)    // represents: z, { (x, y) }
     *
     * If we want to model finding sets a in A that are a subet of b in B, then we know
     * |a intersect b| = |a| is sufficient. 
     *
     * Same parameters as intersectQuery, minus countsTableWhere.
     */
	static def selectWhereSubsetOf(Map kwargs = [:], groovy.sql.Sql sql, tableA, tableB, setColumns) {
        return intersectQuery(kwargs, sql, tableA, tableB, setColumns) { intersectSize, ASizeQuery, BSizeQuery ->
            return """\
                |$intersectSize = (
                |    $ASizeQuery
                |)
            """.stripMargin()
        }
	}

    /** Given a rowTable containing groups of rows with identical values from fields in groupBy, insert those 
     * groups as single rows in columnTable.
     *
     * columnMap is used to map groups in rowTable to single rows in columnTable. If the number of 
     * rows is smaller than the number of columns we're mapping to, then the remaining columns are 
     * filled with nulls.
     *
     * For example:
     *
	 * columnMap = ['x':'x', 'y':['y1', 'y2']]
     *
     * groupBy = ['x']
     * 
     * rowTable:
	 * A(x, y) 
	 *   1  2  
	 *   1  3
     *
     * columnTable:
     * B(x, y1, y2)
     *   1  2   3
     *
     * @param rowTable
     * @param columnTable 
     * @param columnMap
     * a map like [rowTableField:columnTableField] or [rowTableField:['columnTableField1', 'columnTableField2']
     * @param kwargs.orderRowsBy
     * if a column in rowTable maps to multiple columns in columnTable, order which column we insert 
     * the values in by the values themselves.
     * @param kwargs.badGroup
     * a function of type ( [rowTableRow] -> ) where the size of [rowTableRow] exceeds the maximum 
     * number of columns we're mapping to for a given rowTable field.
     */
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

    /* Workaround crappy handling of null sqlParams in methods for groovy.sql.Sql.
     */
    private static def _sql(Map kwargs = [:], sqlMethod, String stmt) {
        if (kwargs.sqlParams != null) {
            sqlMethod stmt, kwargs.sqlParams
        } else {
            sqlMethod stmt
        }
    }

    /** Helper function for generating SQL strings for potentially null values.
     *
     * e.g.
     * def someValueMaybeNull = null
     * _(someValueMaybeNull) == '' 
     * someValueMaybeNull = 'id = :id'
     * _(someValueMaybeNull, return: { clause -> "and $clause"}) == 'and id = :id' 
     *
     * @param kwargs.default 
     * what to return if value == kwargs.null (default: '')
     * @param kwargs.null 
     * what constitues a missing value (when to return kwargs.default)
     * @param kwargs.return
     * if value != kwargs.null, return the result of calling kwargs.return(value)
     */
    static def _(Map kwargs = [:], value) {
        if (kwargs.default == null) { kwargs.default = '' }
        if (!kwargs.containsKey('null')) { kwargs.null = null }
        if (kwargs.return == null) { kwargs.return = { x -> x } }
        if (value != kwargs.null) {
            return kwargs.return(value)
        } else {
            return kwargs.default
        }
    }
	
    /** Generate a SQL string for inserting a query into a table when there may be primary / unique 
     * key duplication.
     *
     * @param intoTable
     * table to insert into
     * @param query
     * select query to insert
     * @param kwargs.onDuplicateKey
     * one of:
     * 1) 'discard': keep the first row from the query and ignore any others with duplicate keys
     * 2) 'update': keep the last row from the query and ignore any others with duplicate keys
     * 3) function ( duplicate_alias, table_alias -> SQLUpdateString ) that generates the clause for 
     *    updating the table row from the attempted duplicate insert (see "DUPLICATE KEY UPDATE 
     *    [CLAUSE]" at http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html)
     */
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
	
    /* Engines supported by MySQL.
     */
    private static def engines = ['MEMORY', 'MyISAM', 'InnoDB']
    /* Valid values for kwargs.saveAs parameter (other than table engines).
     */
    private static def validSaveAs = engines + ['query', 'existing', 'rows', 'iterator']
    /** Perform an SQL query with parameters specified as a map in kwargs.sqlParams.
     * kwargs.saveAs specifies how the query is handled (whether it is saved a new or existing 
     * table, or returned as a list of maps, or returned as an iterator).
     *
     * @param query
     * the query to execute, possibly with :params from kwargs.sqlParams.
     * @param kwargs.saveAs
     * one of:
     * 1) a MySQL engine (in Sql.engines), specifying the new table to create from this query.
     * 2) 'existing', specifying to insert into an existing table.
     * 3) 'rows', returning the query as a list of maps
     * 4) 'iterator', return an result with a .each method for iterating over each map
     * @param kwargs.intoTable
     * if kwargs.saveAs is a MySQL engine or 'existing', specfies the table we insert into.
     * @param columns
     * if kwargs.saveAs is 'existing', specifies the columns of kwargs.intoTable to insert into. 
     */
    private static def selectAs(Map kwargs = [:], groovy.sql.Sql sql, query, columns) {
        setDefaultKwargs(kwargs)
        if (engines.any { kwargs.saveAs == it }) {
			createTableFromExisting(sql, kwargs.intoTable, 
				saveAs:kwargs.saveAs, 
				query:query, 
				indexColumns:kwargs.indexColumns,
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
	
    /** Insert an iterable of lists or maps into table.
     *
     * @param columns
     * Which columns of table we are inserting.  Optional if rows is an iterable of maps (since we 
     * default to the first row's keySet as the columns).
     * @param table
     * The table to insert into.
     * @param rows
     * An iterable of maps or lists.
     */
	static def insert(groovy.sql.Sql sql, table, columns, rows) {
        if (rows == null || rows instanceof List && rows.size() == 0) {
            return
        }

        File infile = File.createTempFile("${table}_table", '.tsv')
        try {
            int numRows = 0
            infile.withPrintWriter() { w ->
                rows.each { row ->
                    numRows += 1
                    if (columns == null && row instanceof LinkedHashMap) {
                        /* This will happen on the first iteration.
                         */
                        columns = row.keySet()
                    }
                    w.println(
                        (row instanceof LinkedHashMap ? columns.collect { row[it] } : row).collect { value ->
                            // TODO: handle escaping '\t' in value via quoting 
                            if (value == null) {
                                // We need to use \N in the data file to represent a NULL value in mysql
                                // http://stackoverflow.com/questions/2675323/mysql-load-null-values-from-csv-data
                                '\\N'
                            } else {
                                value
                            }
                        }.join('\t')
                    )
                }
            }
            if (numRows > 0) {
                String columnStr = (columns != null && columns.size() > 0) ?
                    '(' + columns.join(', ') + ')' :
                    ''
                sql.execute "load data local infile :infile into table ${table}${columnStr}", [infile: infile.absolutePath]
            }
        } finally {
            infile.delete()
        } 

        // NOTE: this batch insert thing is slow.
        // String stmt = "insert into ${table}${(columns.size() > 0) ? "(${columns.join(', ')})" : ''} values (${qmarks(numCols)})"
        // 
        // sql.withBatch(stmt) { ps ->
        //     rows.each { r -> 
        //         ps.addBatch(r) 
        //     }
        // }
	}
	
    /** Same as other insert but with columns unspecified.
     * That is, just insert into columns in the order in which they are declared in the schema.
     */
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

	private static def hashRowsToListRows(rows, cols) {
		rows.collect { r ->
			cols.collect { r[it] }
		}
	}

    /** Select columns from information_schema.columns.
     * @param kwargs.select
     * columns to select from information_schema.columns
     */
    private static def columnMetadata(Map kwargs = [:], groovy.sql.Sql sql) {
        if (kwargs.select == null) { kwargs.select = ['column_name'] }
		_sql(kwargs, sql.&rows, """\
			|select ${ kwargs.select.join(', ') }
			|from information_schema.columns 
			|where table_schema = database()
            |""".stripMargin())
    }

    /** Return metadata about tables in this database as a map.
     * In particular, returns a map like:
     * [
     *     table1: [
     *         // includes primary key columns
     *         columns:    [col1, col2, ...],
     *         // just primary key columns
     *         primaryKey: [col1, col2],
     *     ]
     * ]
     */
    static Map tblColumns(Map kwargs = [:], groovy.sql.Sql sql) {
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
	
    /** Return a list of columns belonging to this table.
     *
     * @param kwargs.where
     * Additional where clause condition on information_schema.columns.
     */
	static List tableColumns(Map kwargs = [:], groovy.sql.Sql sql, table) {
        kwargs.sqlParams = (kwargs.sqlParams ?: [:]) + [table: table]
		hashRowsToListRows(_sql(kwargs, sql.&rows, """\
			|select column_name 
			|from information_schema.columns 
			|where table_schema = database() and
			|      table_name = :table
			|	   ${_(kwargs.where, return: { "and ( $it )" })}""".stripMargin()),
			['column_name']).collect { it[0] }
	}

    /** Wrapper for groovy.sql.Sql.eachRow that replaces the row's keys (the columns returned by 
     * query at positions 1..n) with kwargs.names 1..n.
     *
     * The result is returned as an iterable.
     *
     * This is useful for queries that return columns with the same field name (from different 
     * tables), since these can be distinguished using groovy.sql.Sql.eachRow, whereas identical 
     * column names get merged in groovy.sql.Sql.rows.
     *
     * @param query
     * the query to execute, possibly with :params from kwargs.sqlParams.
     * @param names
     * a list of names the same size as the rows returned by query.
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
