package haplorec.util;

import haplorec.util.Sql
import haplorec.util.dependency.DependencyGraphBuilder
import haplorec.util.dependency.Dependency

public class Haplotype {
    private static def setDefaultKwargs(Map kwargs) {
        def setDefault = { property, defaultValue -> 
            if (kwargs[property] == null) {
                kwargs[property] = defaultValue
            }
        }
        setDefault('saveAs', 'existing')
    }

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
    static def geneHaplotypeToGenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.genotype == null) { kwargs.genotype = 'job_genotype' }
        if (kwargs.geneHaplotype == null) { kwargs.geneHaplotype = 'job_gene_haplotype' }
		// groupedRowsToColumns(sql, kwargs.geneHaplotype, kwargs.genotype)
		// fill job_genotype using job_gene_haplotype, by mapping groups of 2 haplotypes (i.e. biallelic genes) to single job_genotype rows
		Sql.groupedRowsToColumns(sql, kwargs.geneHaplotype, kwargs.genotype,
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
    static def genePhenotypeToDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.drugRecommendation == null) { kwargs.drugRecommendation = 'job_drug_recommendation' }
        if (kwargs.genePhenotype == null) { kwargs.genePhenotype = 'job_gene_phenotype' }
        return Sql.selectWhereSetContains(
            sql,
            kwargs.genePhenotype,
            'gene_phenotype_drug_recommendation',
            ['gene_name', 'phenotype_name'],
            ['drug_recommendation_id'],
            kwargs.drugRecommendation,
            saveAs:kwargs.saveAs, 
            sqlParams:kwargs.sqlParams,
			singlesetWhere:"${kwargs.genePhenotype}.job_id = :job_id",
        )
    }

    static def snpToGeneHaplotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.geneHaplotype == null) { kwargs.geneHaplotype = 'job_gene_haplotype' }
        if (kwargs.variant == null) { kwargs.variant = 'job_variant' }
        return Sql.selectWhereSetContains(
            sql,
            kwargs.variant,
            'gene_haplotype_variant',
            ['snp_id', 'allele'],
            ['gene_name', 'haplotype_name'],
            kwargs.geneHaplotype,
            saveAs:kwargs.saveAs, 
            sqlParams:kwargs.sqlParams,
        )
    }

    static def genotypeToDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.drugRecommendation == null) { kwargs.drugRecommendation = 'job_drug_recommendation' }
        if (kwargs.genotype == null) { kwargs.genotype = 'job_genotype' }
        return Sql.selectWhereSetContains(
            sql,
            kwargs.genotype,
            'genotype_drug_recommendation',
            ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            ['drug_recommendation_id'],
            kwargs.drugRecommendation,
            saveAs:kwargs.saveAs, 
            sqlParams:kwargs.sqlParams,
        )
    }


	// inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
	static def genotypeToGenePhenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        if (kwargs.genePhenotype == null) { kwargs.genePhenotype = 'job_gene_phenotype' }
        if (kwargs.genotype == null) { kwargs.genotype = 'job_genotype' }
		return Sql.selectAs(sql, """\
			select :job_id, gene_name, phenotype_name from ${kwargs.genotype} 
			join genotype_phenotype using (gene_name, haplotype_name1, haplotype_name2)""".toString(),
			['job_id', 'gene_name', 'phenotype_name'],
			saveAs:kwargs.saveAs,
			intoTable:kwargs.genePhenotype,
            sqlParams:kwargs.sqlParams)
	}

	static def createVariant(Map kwargs = [:], groovy.sql.Sql sql) {
		setDefaultKwargs(kwargs)
		Sql.insert(sql, kwargs.variant, ['snp_id', 'allele'], kwargs.variants)
	}
	
    // TODO: accept jobId to rerun parts of the pipeline 
	// - delete existing rows in a table before creating rows
	// - optimization: figure out what tables are already "built" (select count(*) from $table where job_id = :job_id) and pass it to build()
	static def drugRecommendations(Map kwargs = [:], groovy.sql.Sql sql) {
		if (kwargs.pipelineJobTable == null) { kwargs.pipelineJobTable = 'job' }
		if (kwargs.newPipelineJob == null) { kwargs.newPipelineJob = true }
		def tableKey = { defaultTable ->
			defaultTable.replaceFirst(/^job_/,  "")
						.replaceAll(/_(\w)/, { it[0][1].toUpperCase() })
		}
		def tbl = [
			'job_drug_recommendation',
			'job_gene_haplotype',
			'job_gene_phenotype',
			'job_genotype',
			'job_variant',
		].inject([:]) { m, defaultTable ->
			m[tableKey(defaultTable)] = defaultTable
			m
		}

        if (kwargs.jobId == null) {
            List sqlParamsColumns = (kwargs.sqlParams?.keySet() ?: []) as List
            def keys = Sql.sqlWithParams sql.&executeInsert, """\
                insert into job(${sqlParamsColumns.collect { ":$it" }.join(', ')}) 
                values(${(['?']*sqlParamsColumns.size()).join(', ')})""".toString(), 
                kwargs.sqlParams
                kwargs.jobId = keys[0][0]
        }

        def pipelineKwargs = tbl + [
            sqlParams:[
                job_id:kwargs.jobId,
            ]
        ]
		
		def insertOrGenerateRows = { outputTable, Closure generateRows, additionalKwargs = null ->
			def rowsKey = outputTable + 's'
			if (kwargs[rowsKey] != null) {
				def rows = kwargs[rowsKey]
				Sql.insert(sql, kwargs[outputTable], rows)
			} else {
				generateRows((additionalKwargs != null) ? pipelineKwargs + additionalKwargs : pipelineKwargs, sql)
			}
		}

        def builder = new DependencyGraphBuilder()
        Dependency drugRecommendation = builder.dependency(id: tbl.drugRecommendation, target: tbl.drugRecommendation, rule: { ->
            insertOrGenerateRows('drugRecommendation', this.&genotypeToDrugRecommendation)
            /* TODO: make sure this handles duplicates appropriately, either by filtering them out,
             * or by indicating whether the recommendation is from genotype or phenotype information
             */
            insertOrGenerateRows('drugRecommendation', this.&genePhenotypeToDrugRecommendation, [saveAs:'existing'])
        }) {
            dependency(id: tbl.genotype, target: tbl.genotype, rule: { ->
                /* TODO: specify a way of dealing with tooManyHaplotypes errors
                 */
                insertOrGenerateRows('genotype', this.&geneHaplotypeToGenotype)
            }) {
                dependency(id: tbl.geneHaplotype, target: tbl.geneHaplotype, rule: { ->
                    insertOrGenerateRows('geneHaplotype', this.&snpToGeneHaplotype)
                }) {
                    dependency(id: tbl.variant, target: tbl.variant, rule: { ->
                        createVariant(pipelineKwargs, sql)
                    })
                }
            }
            dependency(id: tbl.genePhenotype, target: tbl.genePhenotype, rule: { ->
                insertOrGenerateRows('genePhenotype', this.&genotypeToGenePhenotype)
            }) {
                dependency(refId: tbl.genotype)
            }
        }
        drugRecommendation.build()

	}
	
}
