package haplorec.util;

import groovy.sql.Sql

public class Haplotype {
    private static def DEFAULT_SAVE_AS = 'MyISAM'
	private static def DEFAULT_ENGINE_SAVE_AS = 'MyISAM'

    // static def snpsToHaplotypes(String url, String username, String password, String driver = "com.mysql.jdbc.Driver") {
    //     return snpsToHaplotypes(Sql.newInstance(url, username, password, driver))
    // }

    // TODO: geneHaplotypeToGenotype: { (GeneName, HaplotypeName) } -> { (GeneName, HaplotypeName, HaplotypeName) }
    // where error if # of HaplotypeName for a GeneName >= 3, (null, HaplotypeName) if # == 1, else (HaplotypeName, HaplotypeName)

	/* Keyword Arguments:
	 * 
	 * tooManyHaplotypes: a Closure of type ( GeneName: String, [HaplotypeName: String] -> void )
	 * which represents a list of haplotypes (that is, 
	 * which processes genes for which more than 2 haplotypes were seen 
	 * (i.e. our assumption that the gene has a biallelic genotype has failed).
	 * default: ignore such cases
	 */
    static def geneHaplotypeToGenotype(Map kwargs = [:], Sql sql, saveAs = DEFAULT_SAVE_AS, inputGeneHaplotype = 'input_gene_haplotype', intoTable = 'input_genotype') {
		// groupedRowsToColumns(sql, inputGeneHaplotype, inputGenotype)
		// create the input_genotype table using existing data types from input_gene_haplotype
		if (kwargs.tooManyHaplotypes == null) { kwargs.tooManyHaplotypes = { g -> } }
		createTableFromExisting(sql, intoTable, saveAs, 
			query:"select gene_name, haplotype_name as haplotype_name1, haplotype_name as haplotype_name2 from input_gene_haplotype",
			dontRunQuery:true,
			indexColumns:['gene_name', 'haplotype_name1', 'haplotype_name2'])
		// fill input_genotype using input_gene_haplotype, by mapping groups of 2 haplotypes (i.e. biallelic genes) to single input_genotype rows
		groupedRowsToColumns(sql, inputGeneHaplotype, intoTable,
			'gene_name', 
			['gene_name':'gene_name', 'haplotype_name':['haplotype_name1', 'haplotype_name2']],
			orderRowsBy: ['haplotype_name'],
			badGroup: (kwargs.tooManyHaplotypes == null) ? null : { group ->
				def gene = group[0]['gene_name']
				assert group.collect { row -> row['gene_name'] }.unique().size() == 1 : "all haplotypes belong to the same gene"
				def haplotypes = group.collect { row -> row['haplotype_name'] }
				kwargs.tooManyHaplotypes(gene, haplotypes)
			})
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
			indexColumns: ['gene_name', 'haplotype_name'],
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
	
	
	// inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
	static def genotypeToGenePhenotype(Sql sql, saveAs = DEFAULT_SAVE_AS, inputGenotype = 'input_genotype', intoTable = 'input_gene_phenotype') {
		return createTableFromExisting(sql, intoTable, saveAs,
			query:"""\
			select gene_name, phenotype_name from ${inputGenotype} 
			join gene_phenotype using (gene_name, haplotype_name1, haplotype_name2)""".toString(),
			indexColumns:['gene_name', 'phenotype_name'])
	}
	
	// columnMap = ['x':'x', 'y':['y1', 'y2']]
	// A(x, y) B(x, y1, y2)
	//   1  2    1  2   3
	//   1  3
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
    private static def selectWhereSetContains(Map kwargs = [:], Sql sql, singlesetTable, multisetTable, setColumns, multisetGroupColumns, saveAs, intoTable) {
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
	private static def createTableFromExisting(Map kwargs = [:], Sql sql, newTable, saveAs = DEFAULT_ENGINE_SAVE_AS) {
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
