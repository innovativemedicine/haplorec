package haplorec.util;

import groovy.sql.Sql

public class Haplotype {
    private static def DEFAULT_SAVE_AS = 'query'

	// static def snpsToHaplotypes(String url, String username, String password, String driver = "com.mysql.jdbc.Driver") {
	// 	return snpsToHaplotypes(Sql.newInstance(url, username, password, driver))
	// }

	static def snpsToHaplotypes(Sql sql, saveAs = DEFAULT_SAVE_AS, inputSnps = 'input_snps', intoTable = 'input_gene_haplotype') {
        return selectWhereSetContains(
            sql,
            inputSnps,
            'genotype_drug_recommendation',
            ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            ['drug_recommendation_id'],
            saveAs, 
            intoTable,
        )
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
