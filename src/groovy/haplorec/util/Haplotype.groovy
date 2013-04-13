package haplorec.util;

import haplorec.util.Sql
import haplorec.util.dependency.DependencyGraphBuilder
import haplorec.util.dependency.Dependency

public class Haplotype {
    private static def ambiguousVariants = 'job_patient_variant'
    private static def unambiguousVariants = 'job_patient_chromosome_variant'
    private static def defaultTables = [
        variant            : ambiguousVariants,
        genePhenotype      : 'job_patient_gene_phenotype',
        genotype           : 'job_patient_genotype',
        geneHaplotype      : 'job_patient_gene_haplotype',
        drugRecommendation : 'job_patient_drug_recommendation',
        job                : 'job',
    ]
    private static Set<CharSequence> jobTables = new HashSet(defaultTables.values() + [ambiguousVariants, unambiguousVariants])
	
    private static def setDefaultKwargs(Map kwargs) {
        def setDefault = { property, defaultValue -> 
            if (kwargs[property] == null) {
                kwargs[property] = defaultValue
            }
        }
        setDefault('saveAs', 'existing')
        defaultTables.keySet().each { tblName ->
            setDefault(tblName, defaultTables[tblName]) 
        }
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
			},
			sqlParams:kwargs.sqlParams,
			rowTableWhere:"${kwargs.geneHaplotype}.job_id = :job_id",
		)
    }

    // inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
    static def genePhenotypeToDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
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

    static def unambiguousVariantToGeneHaplotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        return Sql.selectWhereSetContains2(
            sql,
            kwargs.variant,
            'gene_haplotype_variant',
			['snp_id', 'allele'],
            tableAGroupBy: ['patient_id', 'physical_chromosome'],
            tableBGroupBy: ['gene_name', 'haplotype_name'],
			select: [':job_id', 'patient_id', 'gene_name', 'haplotype_name'],
            intoTable: kwargs.geneHaplotype,
            saveAs: kwargs.saveAs, 
            sqlParams: kwargs.sqlParams,
			tableAWhere: { t -> "${t}.job_id = :job_id" },
        )
    }

    static def genotypeToDrugRecommendation(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
        return Sql.selectWhereSetContains(
            sql,
            kwargs.genotype,
            'genotype_drug_recommendation',
            ['gene_name', 'haplotype_name1', 'haplotype_name2'],
            ['drug_recommendation_id'],
            kwargs.drugRecommendation,
            saveAs:kwargs.saveAs, 
            sqlParams:kwargs.sqlParams,
			singlesetWhere:"${kwargs.genotype}.job_id = :job_id",
        )
    }


	// inputGenePhenotype = create table(gene_name, phenotype_name, index(gene_name, phenotype_name))
	static def genotypeToGenePhenotype(Map kwargs = [:], groovy.sql.Sql sql) {
        setDefaultKwargs(kwargs)
		return Sql.selectAs(sql, """\
			select :job_id, gene_name, phenotype_name from ${kwargs.genotype} 
			join genotype_phenotype using (gene_name, haplotype_name1, haplotype_name2)
			where ${kwargs.genotype}.job_id = :job_id""".toString(),
			['job_id', 'gene_name', 'phenotype_name'],
			saveAs:kwargs.saveAs,
			intoTable:kwargs.genePhenotype,
            sqlParams:kwargs.sqlParams,
		)
	}

	static def createVariant(Map kwargs = [:], groovy.sql.Sql sql) {
		setDefaultKwargs(kwargs)
		Sql.insert(sql, kwargs.variant, ['snp_id', 'allele'], kwargs.variants)
	}
	
    // TODO: accept jobId to rerun parts of the pipeline 
	// - delete existing rows in a table before creating rows
	// - optimization: figure out what tables are already "built" (select count(*) from $table where job_id = :job_id) and pass it to build()
	static def drugRecommendations(Map kwargs = [:], groovy.sql.Sql sql) {
		if (kwargs.newPipelineJob == null) { kwargs.newPipelineJob = true }
		if (kwargs.ambiguousVariants == null) { kwargs.ambiguousVariants = true }
		def tableKey = { defaultTable ->
			defaultTable.replaceFirst(/^job_/,  "")
						.replaceAll(/_(\w)/, { it[0][1].toUpperCase() })
		}
        // default job_* tables
        // dependency target -> sql table
		def tbl = new LinkedHashMap(defaultTables)
		tbl.remove('job')
		if (kwargs.ambiguousVariants) {
			tbl.variant = ambiguousVariants
		} else {
			tbl.variant = unambiguousVariants
		}
//		def tbl = defaultTables.keySet().grep { it != 'job' }.collect { defaultTables[it] }.inject([:]) { m, defaultTable ->
//			m[tableKey(defaultTable)] = defaultTable
//			m
//		}
		
        if (kwargs.jobId == null) {
            // Create a new job
            List sqlParamsColumns = (kwargs.sqlParams?.keySet() ?: []) as List
            def keys = Sql.sqlWithParams sql.&executeInsert, """\
                insert into job(${sqlParamsColumns.collect { ":$it" }.join(', ')}) 
                values(${(['?']*sqlParamsColumns.size()).join(', ')})""".toString(), 
                kwargs.sqlParams
                kwargs.jobId = keys[0][0]
        } else {
            // Given an existing jobId, delete all job_* rows, then rerun the pipeline
            if ((sql.rows("select count(*) as count from ${kwargs.job}".toString()))[0]['count'] == 0) {
                throw new IllegalArgumentException("No such job with job_id ${kwargs.jobId}")
            }
            jobTables.values().each { jobTable ->
                sql.execute "delete from $jobTable where id = :jobId".toString(), kwargs
            }
        }

        def insertDepedencyRows = { table, rows ->
            rows.each { r ->
                r.add(0, kwargs.jobId)
            }
            Sql.insert(sql, tbl[table], rows)
        }
        def pipelineKwargs = tbl + [
            sqlParams:[
                job_id:kwargs.jobId,
            ]
        ]
        def builder = new DependencyGraphBuilder()
        Map dependencies = [:]
        dependencies.drugRecommendation = builder.dependency(id: tbl.drugRecommendation, target: tbl.drugRecommendation, rule: { ->
            genotypeToDrugRecommendation(pipelineKwargs, sql)
            /* TODO: make sure this handles duplicates appropriately, either by filtering them out,
             * or by indicating whether the recommendation is from genotype or phenotype information
             */
            genePhenotypeToDrugRecommendation(pipelineKwargs + [saveAs:'existing'], sql)
        }) {
            dependencies.genotype = dependency(id: tbl.genotype, target: tbl.genotype, rule: { ->
                /* TODO: specify a way of dealing with tooManyHaplotypes errors
                 */
                geneHaplotypeToGenotype(pipelineKwargs, sql)
            }) {
                dependencies.geneHaplotype = dependency(id: tbl.geneHaplotype, target: tbl.geneHaplotype, rule: { ->
                    // TODO: check which variant table we depend on got built (i.e. the ambigious one, or the non-ambiguous one) and call the appropriate snpToGeneHaplotype function that handles 
                    // those cases
                    if (kwargs.ambiguousVariants) {
                        ambiguousVariantToGeneHaplotype(pipelineKwargs, sql)
                    } else {
                        unambiguousVariantToGeneHaplotype(pipelineKwargs, sql)
                    }
                }) {
                    if (kwargs.ambiguousVariants) {
                        // TODO: if the input variants are of the ambiguous variety, create a dependencies.variant that handles them appropriately (i.e. with error checking / filter of ambigious cases)
                        throw new RuntimeException("Still need to implement handling variant lists that ambiguously identify which physical chromosome they occur on")
                        // dependencies.variant = dependency(id: tbl.variant, target: tbl.variant, rule: { ->
                        // TODO: split rs# AG into (rs#, A), (rs#, G) rows
                    } else {
                        // else, create a dependencies.variant that handles them appropriately (i.e. no error checking)
                        dependencies.variant = dependency(id: tbl.variant, target: tbl.variant, rule: { ->
                            insertDepedencyRows('variant', kwargs.variants)
                        })
                    }
                }
            }
            dependencies.genePhenotype = dependency(id: tbl.genePhenotype, target: tbl.genePhenotype, rule: { ->
                genotypeToGenePhenotype(pipelineKwargs, sql)
            }) {
                dependency(refId: tbl.genotype)
            }
        }

        // For datasets that are already provided, insert their rows into the approriate job_* table, and mark them as built
        Set<Dependency> built = []
        dependencies.keySet().each { table ->
			def rowsKey = table + 's'
			if (kwargs[rowsKey] != null && rowsKey != 'variants') {
				def rows = kwargs[rowsKey]
                insertDepedencyRows(table, rows)
                built.add(dependencies[table])
			}
        }

        dependencies.drugRecommendation.build(built)
	}
	
}
