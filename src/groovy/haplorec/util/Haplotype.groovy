package haplorec.util;

import haplorec.util.Sql
import haplorec.util.dependency.DependencyGraphBuilder
import haplorec.util.dependency.Dependency

public class Haplotype {
    private static def DEFAULT_SAVE_AS = 'MyISAM'

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
	// TODO: problem is funky placement of '=' kwargs; stop using those things and just use Map kwargs; also fix query below not to use input_gene_haplotype
    static def geneHaplotypeToGenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.intoTable == null) { kwargs.intoTable = 'input_genotype' }
        if (kwargs.geneHaplotype == null) { kwargs.geneHaplotype = 'input_gene_haplotype' }
		// groupedRowsToColumns(sql, kwargs.geneHaplotype, kwargs.genotype)
		// create the input_genotype table using existing data types from input_gene_haplotype
		if (kwargs.tooManyHaplotypes == null) { kwargs.tooManyHaplotypes = { g -> } }
		Sql.createTableFromExisting(sql, kwargs.intoTable, 
			query:"select gene_name, haplotype_name as haplotype_name1, haplotype_name as haplotype_name2 from gene_haplotype_variant",
			dontRunQuery:true,
			indexColumns:['gene_name', 'haplotype_name1', 'haplotype_name2'],
            saveAs:kwargs.saveAs)
		// fill input_genotype using input_gene_haplotype, by mapping groups of 2 haplotypes (i.e. biallelic genes) to single input_genotype rows
		Sql.groupedRowsToColumns(sql, kwargs.geneHaplotype, kwargs.intoTable,
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

    private static def setDefaultKwargs(Map kwargs) {
        def setDefault = { property, defaultValue -> 
            if (kwargs[property] == null) {
                kwargs[property] = defaultValue
            }
        }
        setDefault('saveAs', DEFAULT_SAVE_AS)
    }

    // inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
    static def genePhenotypeToDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.intoTable == null) { kwargs.intoTable = 'input_drug_recommendation' }
        if (kwargs.genePhenotype == null) { kwargs.genePhenotype = 'input_gene_phenotype' }
        return Sql.selectWhereSetContains(
            sql,
            kwargs.genePhenotype,
            'gene_phenotype_drug_recommendation',
            ['gene_name', 'phenotype_name'],
            ['drug_recommendation_id'],
            kwargs.intoTable,
            saveAs:kwargs.saveAs, 
        )
    }

    static def snpToGeneHaplotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.intoTable == null) { kwargs.intoTable = 'input_gene_haplotype' }
        if (kwargs.variant == null) { kwargs.variant = 'input_variant' }
        return Sql.selectWhereSetContains(
            sql,
            kwargs.variant,
            'gene_haplotype_variant',
            ['snp_id', 'allele'],
            ['gene_name', 'haplotype_name'],
			indexColumns: ['gene_name', 'haplotype_name'],
            kwargs.intoTable,
            saveAs:kwargs.saveAs, 
        )
    }

    static def genotypeToDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.intoTable == null) { kwargs.intoTable = 'input_drug_recommendation' }
        if (kwargs.genotype == null) { kwargs.genotype = 'input_genotype' }
        return Sql.selectWhereSetContains(
            sql,
            kwargs.genotype,
            'genotype_drug_recommendation',
            ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            ['drug_recommendation_id'],
            kwargs.intoTable,
            saveAs:kwargs.saveAs, 
        )
    }
	
	
	// inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
	static def genotypeToGenePhenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.intoTable == null) { kwargs.intoTable = 'input_gene_phenotype' }
        if (kwargs.genotype == null) { kwargs.genotype = 'input_genotype' }
		return Sql.createTableFromExisting(sql, kwargs.intoTable, 
			query:"""\
			select gene_name, phenotype_name from ${kwargs.genotype} 
			join genotype_phenotype using (gene_name, haplotype_name1, haplotype_name2)""".toString(),
			indexColumns:['gene_name', 'phenotype_name'],
			saveAs:kwargs.saveAs)
	}
	
	static def createVariant(Map kwargs = [:], groovy.sql.Sql sql, table, rows) {
		setDefaultKwargs(kwargs)
		def columns = ['snp_id', 'allele']
		Sql.createTableFromExisting(sql, table,
			columns: columns,
			existingTable: 'gene_haplotype_variant',
			indexColumns: columns)
		Sql.insert(sql, table, columns, rows)
	}
	
	/*
	 * TODO: make it so we can provide an iterable set of rows to use for an input_* table (thereby skipping the building of that table)
	 */
	static def drugRecommendations(Map kwargs = [:], groovy.sql.Sql sql) {
		if (kwargs.pipelineJobTable == null) { kwargs.pipelineJobTable = 'pipeline_job' }
		if (kwargs.newPipelineJob == null) { kwargs.newPipelineJob = true }
		if (kwargs.inputTables == null) {
			kwargs.inputTables = [:]
		}
		def tableKey = { defaultTable ->
			defaultTable.replaceFirst(/^input_/,  "")
						.replaceAll(/_(\w)/, { it[0][1].toUpperCase() })
		}
		def defaultTableMap = [
			'input_drug_recommendation',
			'input_gene_haplotype',
			'input_gene_phenotype',
			'input_genotype',
			'input_genotype_phenotype',
			'input_variant',
		].inject([:]) { m, defaultTable ->
			m[tableKey(defaultTable)] = defaultTable
			m
		}
		def tableValue = { tableName, id ->
			(
				(kwargs.inputTables[tableName] != null) ?
					kwargs.inputTables[tableName] :
					defaultTableMap[tableName]
			) +	(
				(kwargs.newPipelineJob) ?
					"_$id" :
					''
			)
		}
		
		def buildDrugRecommendations = { tbl ->
			def builder = new DependencyGraphBuilder()
			Dependency drugRecommendation = builder.dependency(id: tbl.drugRecommendation, target: tbl.drugRecommendation, rule: { ->
				genotypeToDrugRecommendation(sql, genotype: tbl.genotype, intoTable: tbl.drugRecommendation)
				/* TODO: make sure this handles duplicates appropriately, either by filtering them out,
				 * or by indicating whether the recommendation is from genotype or phenotype information
				 */
				genePhenotypeToDrugRecommendation(sql, saveAs:'existing', genePhenotype: tbl.genePhenotype, intoTable: tbl.drugRecommendation)
			}) {
				dependency(id: tbl.genotype, target: tbl.genotype, rule: { ->
					/* TODO: specify a way of dealing with tooManyHaplotypes errors
					 */
					geneHaplotypeToGenotype(sql, geneHaplotype: tbl.geneHaplotype, intoTable: tbl.genotype)
				}) {
					dependency(id: tbl.geneHaplotype, target: tbl.geneHaplotype, rule: { ->
						snpToGeneHaplotype(sql, variant: tbl.variant, intoTable: tbl.geneHaplotype)
					}) {
						dependency(id: tbl.variant, target: tbl.variant, rule: { ->
							createVariant(sql, tbl.variant, kwargs.variants)
						})
					}
				}
				dependency(id: tbl.genePhenotype, target: tbl.genePhenotype, rule: { ->
					genotypeToGenePhenotype(sql, genotype: tbl.genotype, intoTable: tbl.genePhenotype)
				}) {
					dependency(refId: tbl.genotype)
				}
			}
			try {
				drugRecommendation.build()
			} finally {
				/* Delete tables (except for the final drug recommendations)
				 * TODO: we should insert the drug recommendations into a persistent table
				 */
				tbl.keySet().grep { it != 'drugRecommendation' }.each { tableName ->
					sql.execute "drop table if exists ${tbl[tableName]}".toString()
				}
			}
		}
		
		def buildTbl = { id = null -> 
			defaultTableMap.keySet().inject([:]) { m, tableName ->
				m[tableName] = tableValue(tableName, id)
				m
			}
		}
		if (kwargs.newPipelineJob) {
			Sql.withUniqueId(sql, kwargs.pipelineJobTable) { id ->
				def tbl = buildTbl(id)
				buildDrugRecommendations(tbl)
			}
		} else {
			def tbl = buildTbl()
			buildDrugRecommendations(tbl)
		}
	}
	
}
