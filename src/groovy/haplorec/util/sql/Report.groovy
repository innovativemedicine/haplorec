package haplorec.util.sql

import haplorec.util.Row
import haplorec.util.Sql
import static haplorec.util.Sql._

class Report {

    /** Performs a SQL join as specified from kwargs.select and kwargs.join, but removes repeated 
     * information (as a result of joining rows) based on kwargs.duplicateKey.
     *
     * All tables involved in the join are aliased to the first characters of the '_' separated parts 
     * that make up their name. For example, the table job_patient_variant would be aliased to jpv.
     *
     * Repeated information is considered to be consecutive rows with groups whose 
     * kwargs.duplicateKey is the same.
     *
     * @param kwargs.select a map like [table_name: ['field1', ..., 'fieldn'] which specifies which 
     * fields to select (and in what order)
     * @param kwargs.join a map like [table_name: ["JOIN TYPE", "JOIN CLAUSE"]], specifying the join
     * @param kwargs.where a string representing the where clause, possibly using :some_param 
     * specified in kwargs.sqlParams
     * @param kwargs.sqlParams a map like [some_param: 'a_value'] which will be passed as params 
     * when executing the generated SQL string
     * @param kwargs.duplicateKey a map like [table1: ['table1field', ..., [table2: ['table2field', ...]]]] 
     * specifying what consecutive rows with groups with the same duplicateKey's are considered 
     * duplicate groups (if a table isn't specified we default to its primary key)
     * @param kwargs.fillWith a function of type ( row, column -> value ) that replaces duplicate 
     * fields (hence removed) in the join (default: return null).
     * @param kwargs.replace a function of type ( row, column -> boolean ) indicates whether to 
     * replace row[column] with kwargs.fillWith(row, column) (default: if the row is missing the 
     * column, fill it with fillWith)
     */
    private static def condensedJoin(Map kwargs = [:], groovy.sql.Sql sql) {
        if (kwargs.fillWith == null) {
            // kwargs.fillWith = null is the default
        }
        if (kwargs.replace == null) {
            // kwargs.replace = null is the default
        }
        if (kwargs.duplicateKey == null) {
            kwargs.duplicateKey = [:]
        }
        def tables = Sql.tblColumns(sql)

        /* Table name to alias mapping.
         */
        def alias = ( kwargs.select.keySet() + kwargs.join.keySet() ).inject([:]) { m, table -> m[table] = aliafy(table); m }
        /* Alias to table mapping.
         */
        def aliasToTable = alias.keySet().inject([:]) { m, k -> m[alias[k]] = k; m }

        def joinTables = kwargs.join.keySet()
        def tablesNotInJoin = new HashSet(kwargs.select.keySet())
        tablesNotInJoin.removeAll(joinTables)
        if (tablesNotInJoin.size() != 1) {
            throw new IllegalArgumentException("There must be exactly one table without a join clause, but saw ${tablesNotInJoin.size()} such tables: ${tablesNotInJoin}")
        }
        def tableNotInJoin = tablesNotInJoin.iterator().next()

        def query = """
        |select
        |${ kwargs.select.keySet().collect { table -> tables[table].columns.collect { "${alias[table]}.$it" }.join(', ') }.join(',\n') }
        |from 
        |${ ( ["$tableNotInJoin ${alias[tableNotInJoin]}"] + kwargs.join.collect { "${it.value[0]} ${it.key} ${alias[it.key]} ${it.value[1]}" } ).join('\n') }
        |${ _(kwargs.where, return: { "where $it" }) }
        |""".stripMargin()[0..-2]

        def colName = { table, column ->
            "$table.$column".toString()
        }

        def rows = Sql.rows(sql, query, 
            sqlParams: kwargs.sqlParams,
            names: kwargs.select.keySet().collect { table -> 
                tables[table].columns.collect { c -> colName(table, c) } 
            }.flatten(),
        )

        def nonNullKeys = { map -> 
            map.keySet().grep { k -> map[k] != null } 
        }
        Row.fill(
            with: kwargs.fillWith, 
            replace: kwargs.replace, 
            // NOTE: Row.collapse assumes ordering of rows is "correct"; that is, consecutive rows 
            // that satisfy 'canCollapse' are actually meant to be collapsed.  To enforce this we 
            // could add an ORDER BY clause to our query but _I_think_ the fetch order ensures it. 
            // I really ought to double check this...
            Row.collapse(
                canCollapse: { header, lastRow, currentRow ->
                    /* We can collapse two rows if:
                     * 1. either of them are empty
                     * 2. their columns do not overlap
                     * 3. the index (into the header) of the first column that occurs in currentRow is 
                     *    after the index of the last column in lastRow
                     */

                    // 1.
                    if (lastRow == [:] || currentRow == [:]) { return true }

                    def lastRowKeys = nonNullKeys(lastRow)
                    def currentRowKeys = nonNullKeys(currentRow)

                    // 2.
                    def intersect = new HashSet(lastRowKeys)
                    intersect.retainAll(currentRowKeys)
                    if (intersect.size() != 0) { return false }

                    // 3.
                    def idx = { column ->
                        def i = 0
                        for (h in header) { 
                            if (h == column) {
                                return i
                            }
                            i += 1
                        }
                    }
                    // NOTE: 1. guarantees first and last will get set
                    def first
                    for (key in currentRowKeys) {
                        first = key
                        break
                    }
                    def last
                    for (key in lastRowKeys) {
                        last = key
                    }
                    return idx(first) > idx(last)

                },
                collapse: { header, lastRow, currentRow ->
                    nonNullKeys(currentRow).each { k ->
                        lastRow[k] = currentRow[k]
                    }
                },
                Row.filter(
                    keep: kwargs.select.collect { kv -> 
                        def (table, columns) = [kv.key, kv.value] 
                        columns.collect { c -> colName(table, c) } 
                    }.flatten(),
                    Row.noDuplicates(rows,
                        // GroupName : [[DuplicateKey], [ColumnsToShow]]
                        kwargs.select.inject([:]) { m, pair ->
                            def (table, columns) = [pair.key, pair.value]
                            def dupkey = (kwargs.duplicateKey[table] != null) ? 
                                kwargs.duplicateKey[table] :
                                tables[table].primaryKey
                            m[table] = [
                                dupkey.collect { c -> 
                                    if (c instanceof Map) {
                                        def tbl = c.keySet().iterator().next()
                                        c[tbl].collect { colName(tbl, it) }
                                    } else {
                                        colName(table, c) 
                                    }
                                }.flatten(),
                                columns.collect { c -> colName(table, c) },
                            ]
                            return m
                        }
                    ),
                )
            )
        )
    }

    /** Refer to Report.condensedJoin about how tables are aliased in kwargs.join.
     * E.g. job_patient_variant would be aliased to jpv.
     */
    private static def aliafy(table) {
        table.replaceAll(
            /(:?^|_)(\w)[^_]*/, 
            { 
                it[2].toLowerCase() 
            })
    }

}
