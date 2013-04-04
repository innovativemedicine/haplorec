package haplorec.util;

import groovy.sql.Sql

public class Haplotype {
    private static def DEFAULT_SAVE_AS = 'query'

    // static def snpsToHaplotypes(String url, String username, String password, String driver = "com.mysql.jdbc.Driver") {
    //     return snpsToHaplotypes(Sql.newInstance(url, username, password, driver))
    // }

    // TODO: geneHaplotypeToGenotype: { (GeneName, HaplotypeName) } -> { (GeneName, HaplotypeName, HaplotypeName) }
    // where error if # of HaplotypeName for a GeneName >= 3, (null, HaplotypeName) if # == 1, else (HaplotypeName, HaplotypeName)
    static def geneHaplotypeToGenotype(Sql sql, saveAs = DEFAULT_SAVE_AS, inputGeneHaplotype = 'input_gene_haplotype', intoTable = 'input_genotype') {
    }

    private static def invertedMap(Map m) {
        m.keySet().inject([:]) {
            map, key ->    
            def value = m[key]
            if ( value instanceof java.util.List ) {
                value.each { v ->
                    map[v] = key
                }
            } else {
                map[value] = key
            }
            map
        }
    }

    // columnMap = ['x':'x', 'y':['y1', 'y2']]
    // A(x, y) B(x, y1, y2)
    //   1  2    1  2   3
    //   1  3
	//http://stackoverflow.com/questions/13708718/groovy-binding-of-keyword-argument
    private static def groupedRowsToColumns(Map kwargs = [:], Sql sql, rowTable, columnTable, groupBy, columnMap) {
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

    // inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
    static def genePhenotypeToDrugRecommendation(Sql sql, saveAs = DEFAULT_SAVE_AS, inputGenePhenotype = 'input_gene_phenotype', intoTable = 'input_drug_recommendation') {
        return selectWhereSetContains(
            sql,
            inputGenePhenotype,
            'gene_phenotype_drug_recommendation',
            ['gene_name', 'phenotype_name'],
            ['drug_recommendation_id'],
            saveAs, 
            intoTable,
        )
    }

    static def snpToGeneHaplotype(Sql sql, saveAs = DEFAULT_SAVE_AS, inputVariant = 'input_variant', intoTable = 'input_gene_haplotype') {
        return selectWhereSetContains(
            sql,
            inputVariant,
            'gene_haplotype_variant',
            ['snp_id', 'allele'],
            ['gene_name', 'haplotype_name'],
            'query', 
            intoTable,
        )
    }

    static def genotypeToDrugRecommendation(Sql sql, saveAs = DEFAULT_SAVE_AS, inputGenotype = 'input_genotype', intoTable = 'input_drug_recommendation') {
        return selectWhereSetContains(
            sql,
            inputGenotype,
            'genotype_drug_recommendation',
            ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            ['drug_recommendation_id'],
            'query', 
            intoTable,
        )
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

    // saveAs = (MEMORY|MyISAM|query|existing)
    private static def selectWhereSetContains(Sql sql, singlesetTable, multisetTable, setColumns, multisetGroupColumns, saveAs, intoTable) {
        def setColumnsStr = setColumns.join(', ')
        def multisetGroupColumnsStr = multisetGroupColumns.join(', ')
        def query = { into = '' ->
            // alias for outer multisetTable of query
            def outer = "outer"
            def whereEitherSetContainsStr = whereEitherSetContains(
                "select $setColumnsStr from $singlesetTable", 
                "select $setColumnsStr from $multisetTable where " +
                    multisetGroupColumns.collect { "$it = $outer.$it" }.join('and '),
                setColumns
            )
            return """\
            select distinct $multisetGroupColumnsStr from $multisetTable $outer
                $into
                where $whereEitherSetContainsStr 
            """
        }
        return selectAs(sql, query, setColumns, saveAs, intoTable)
    }

    private static def engines = ['MEMORY', 'MyISAM']
    private static def validSaveAs = engines + ['query', 'existing']
    private static def selectAs(Sql sql, Closure query, columns, saveAs = 'query', intoTable = null) {
        def columnStr = columns.join(', ')
        if (engines.any { saveAs == it }) {
            def q = query()
            // create the temporary table using the right datatypes
            sql.executeUpdate "create temporary table $intoTable as ($q) limit 0"
            // create the temporary table using the right datatypes
            sql.executeUpdate "alter table $intoTable engine = $saveAs"
            def qInsertInto = query('INTO $intoTable')
            sql.executeUpdate qInsertInto
            sql.executeUpdate "alter table $intoTable add index ($columnStr)"
        } else if (saveAs == 'query') {
            return query()
        } else if (saveAs == 'existing') {
            def qInsertInto = query('INTO $intoTable')
            sql.executeUpdate qInsertInto
        } else {
            throw new RuntimeException("Unknown saveAs type for outputting SQL results; saveAs was $saveAs but must be one of " + validSaveAs.join(', '))
        }
    }

}
