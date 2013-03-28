package haplorec.util;

import groovy.sql.Sql

public class Haplotype {
	static def snpsToHaplotypes(String url, String username, String password, String driver = "com.mysql.jdbc.Driver") {
		return snpsToHaplotypes(Sql.newInstance(url, username, password, driver))
	}
	
	static def snpsToHaplotypes(Sql sql) {
		
	}

    // inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
    static def genePhenotypeToDrugRecommendation(Sql sql, inputGenePhenotype, saveAs) {
    }

    static def snpToGeneHaplotype(Sql sql, inputVariants, saveAs) {
    }

    static def genotypeToDrugRecommendation(Sql sql, inputGenotype, saveAs) {
    }

    private static def engines = ['MEMORY', 'MyISAM']

    private static def whereEitherSetContains(queryA, queryB, columns) {
        // refer to src/sql/mysql/subset_query.sql 
        def columnStr = columns.join(', ')
        return """\
        ( 
            select count(*) from (
                select * from ($queryA) A join ($queryB) B using ($columnStr)
            ) tmp ) = least(($queryA), ($queryB))
        )
        """
    }

    // saveAs = (MEMORY|MyISAM|query|existing)
    private static def selectWhereSetContains(Sql sql, singleSubsetTable, multiSubsetTable, columns, saveAs, intoTable) {
        def columnStr = columns.join(', ')
        def query = { into = '' ->
            // alias for outer multiSubsetTable of query
            def outer = "outer"
            def whereEitherSetContainsStr = whereEitherSetContains(
                "select $columnStr from $singleSubsetTable", 
                "select $columnStr from $multiSubsetTable where " +
                    columns.collect { "$it = $outer.$it" }.join('and '),
                columns
            )
            return """\
            select distinct $columnStr from $multiSubsetTable $outer
                $into
                where $whereEitherSetContainsStr 
            """
        }
        return selectAs(sql, query, columns, saveAs, intoTable)
    }

    private static def selectAs(Sql sql, Closure query, columns, saveAs = 'query', intoTable = null) {
        def columnStr = columns.join(', ')
        def validSaveAs = engines + ['query', 'existing']
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
